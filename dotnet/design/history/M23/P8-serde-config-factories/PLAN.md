# Plan: clients built by `Create`, with serializer and deserializer functions in the configuration

Status: implemented (steps S0–S7 below). The owner's decision of 2026-11-19 has two parts: the ten points this
plan started from (ledger rows 553–560) and the answers that came later the same day through the Manager (rows
570, 571, 573, 574, 575), which changed the public shape from constructors to static `Create` methods. The choices
this plan adds are "Proposed" ledger rows, for the owner to accept or change.

Milestone 23, Phase 8. Agent number N=130. Mode A (.NET only): no Rust, C ABI, header, Python or proto change, and
no new P/Invoke. Branch `esabellico/dotnet_async_serdes_and_zero_copy`, on top of M23/P7 (`53f219229`), in the
worktree `kafka-clients-dotnet-serdes`. No upstream; never pushed by an agent. All paths below are under `dotnet/`.

How it ran: a dotnet-actor 130 implemented one step at a time and committed it with `--no-verify` after running
the step's gates (section "Gates per step"). No rule file is edited: the rule sentences this change makes stale
are listed with their replacement in "Draft rule text (not applied)", and until the owner applies them this plan
wins over the rule text it replaces.

## Context

Java's `KafkaProducer(Map)` and `KafkaConsumer(Map)` take no serializer arguments: they read the classes under
`key.serializer` / `value.serializer` (`key.deserializer` / `value.deserializer`), build an instance of each by
reflection and call `configure(configs, isKey)` on it. Java's `KafkaProducer(Map, Serializer, Serializer)` and
`KafkaConsumer(Map, Deserializer, Deserializer)` do the same for an argument that is null. The .NET binding left
both out (ledger 365, "constructor arguments only"): a C# constructor cannot await, and building a class by
reflection closes trimming and native AOT. So a .NET user had to build each serde and, for a configurable one,
call `Configure` by hand with the same configuration the client gets.

The owner's decision of 2026-11-19 reverses that part of ledger 365 without reflection: the value under the key
is a **function that returns the serde**, and the client calls it. The client then configures the instance, as
Java configures the instance it builds, and disposes it at close, as Java closes it. Since an asynchronous serde's
`Configure` may wait (row 571), the client is built by a static `Create`, which the asynchronous clients return as
a `Task`; the synchronous clients have the same `Create`, so the four real clients are built the same way (row
573).

```csharp
var configs = new Dictionary<string, object?>
{
    ["bootstrap.servers"] = "localhost:9092",
    ["key.serializer"] = new Func<ISerializer<string>>(() => new StringSerializer()),
    ["value.serializer"] = new Func<ISerializer<string>>(() => new StringSerializer()),
    ["value.serializer.encoding"] = "UTF-16",
};
using var producer = KafkaProducer<string, string>.Create(configs);

configs["key.serializer"] = new Func<IAsyncSerializer<string>>(() => new AsyncStringSerializer());
configs["value.serializer"] = new Func<IAsyncSerializer<string>>(() => new AsyncStringSerializer());
await using var asyncProducer = await AsyncKafkaProducer<string, string>.Create(configs);
```

## Design

### Public surface

```csharp
// Confluent.Kafka.Producer
public sealed class KafkaProducer<TKey, TValue>
{
    public static KafkaProducer<TKey, TValue> Create(IReadOnlyDictionary<string, object?> configs);
    public static KafkaProducer<TKey, TValue> Create(
        IReadOnlyDictionary<string, object?> configs,
        ISerializer<TKey>? keySerializer,
        ISerializer<TValue>? valueSerializer);
}

public sealed class AsyncKafkaProducer<TKey, TValue>
{
    public static Task<AsyncKafkaProducer<TKey, TValue>> Create(
        IReadOnlyDictionary<string, object?> configs, CancellationToken cancellationToken = default);
    public static Task<AsyncKafkaProducer<TKey, TValue>> Create(
        IReadOnlyDictionary<string, object?> configs,
        IAsyncSerializer<TKey>? keySerializer,
        IAsyncSerializer<TValue>? valueSerializer,
        CancellationToken cancellationToken = default);
}

// Confluent.Kafka.Consumer
public sealed class KafkaConsumer<TKey, TValue>
{
    public static KafkaConsumer<TKey, TValue> Create(IReadOnlyDictionary<string, object?> configs);
    public static KafkaConsumer<TKey, TValue> Create(
        IReadOnlyDictionary<string, object?> configs,
        IDeserializer<TKey>? keyDeserializer,
        IDeserializer<TValue>? valueDeserializer);
}

public sealed class AsyncKafkaConsumer<TKey, TValue>
{
    public static Task<AsyncKafkaConsumer<TKey, TValue>> Create(
        IReadOnlyDictionary<string, object?> configs, CancellationToken cancellationToken = default);
    public static Task<AsyncKafkaConsumer<TKey, TValue>> Create(
        IReadOnlyDictionary<string, object?> configs,
        IAsyncDeserializer<TKey>? keyDeserializer,
        IAsyncDeserializer<TValue>? valueDeserializer,
        CancellationToken cancellationToken = default);
}

// Confluent.Kafka.Common.Serialization
public interface IAsyncSerializerWithConfigure<T> : IAsyncSerializer<T>
{
    ValueTask Configure(IReadOnlyDictionary<string, object?> configs, bool isKey, CancellationToken cancellationToken);
}

public interface IAsyncDeserializerWithConfigure<T> : IAsyncDeserializer<T>
{
    ValueTask Configure(IReadOnlyDictionary<string, object?> configs, bool isKey, CancellationToken cancellationToken);
}

public static class AsyncSerializerExtensions
{
    public static ValueTask Configure<T>(
        this IAsyncSerializer<T> serializer,
        IReadOnlyDictionary<string, object?> configs,
        bool isKey,
        CancellationToken cancellationToken = default);
}

public static class AsyncDeserializerExtensions
{
    public static ValueTask Configure<T>(
        this IAsyncDeserializer<T> deserializer,
        IReadOnlyDictionary<string, object?> configs,
        bool isKey,
        CancellationToken cancellationToken = default);
}
```

- The four real clients have **no public constructor** (row 573). Their only constructor is private and takes the
  native client and the two serdes.
- `Create(configs)` is `Create(configs, null, null)`, as Java's `KafkaProducer(Map)` is `this(configs, null, null)`.
- The four mocks (`MockProducer`, `AsyncMockProducer`, `MockConsumer`, `AsyncMockConsumer`) keep their public
  constructors and have no `Create`: Java's mocks have no configuration route.
- The built-in asynchronous serdes that Java configures (`AsyncStringSerializer`, `AsyncStringDeserializer`,
  `AsyncUUIDSerializer`, `AsyncUUIDDeserializer`) implement the `ValueTask` `Configure` and complete it
  synchronously. The synchronous serde interfaces and their `Configure` are unchanged.

### The decisions

The ten points of the first decision, in their final shape:

1. **A function, not a class.** The value under the key is a parameterless delegate. It is accepted when
   `value is Func<ISerializer<TKey>>` holds (the interface of the client's kind and type argument: `ISerializer`,
   `IAsyncSerializer`, `IDeserializer`, `IAsyncDeserializer`). `Func<out T>` is covariant, so a
   `Func<StringSerializer>` passes.
2. **No reflection.** No `Activator`, no `System.Type`, no class name. A value of another kind (a string, a
   `System.Type`, a function of another interface, a function returning a struct) is a bare `ConfigException` that
   names the key and the expected delegate type.
3. **A missing or null key**, for an argument that is null, is Java's `ConfigException` "Invalid value null for
   configuration key.serializer: must be non-null.", the key before the value, as the first check after a null
   `configs` (`ProducerConfig.appendSerializerToConfig`, `ConsumerConfig.appendDeserializerToConfig`).
4. **`Configure(configs, isKey)`** is called on an instance the client built iff it implements
   `ISerializerWithConfigure<T>` / `IAsyncSerializerWithConfigure<T>` / `IDeserializerWithConfigure<T>` /
   `IAsyncDeserializerWithConfigure<T>`: the key with `true`, then the value with `false`, once each. On the
   asynchronous clients it is awaited, with `Create`'s token. A serde given as an argument is never configured.
   `configs` is a copy of the user's configuration (row 561).
5. **A failure while the serdes are built** (the function returns null or throws, `Configure` throws, the client
   refuses the instance) is a `KafkaException` "Failed to construct kafka producer" / "Failed to construct kafka
   consumer" with the cause as `InnerException`, as Java's constructor wraps what its `try` throws. What Java's
   constructor would hold by then is disposed (row 562).
6. **Order.** The binding's configuration checks and the core's configuration validation come first, as bare
   `ConfigException`s, before any function runs.
7. **Disposal.** A serde the client built is disposed at close exactly as a given one (`SerdeDisposal`), and
   `SerdeGuard.Check` applies to it. An asynchronous client disposes a serde only through `IAsyncDisposable` (row
   575).
8. **The five own-role keys still never reach the core.**
9. **The mocks are unchanged.**
10. **The route really configures**: `["key.serializer"]` holding a function that returns a `StringSerializer`,
    with `["key.serializer.encoding"]` set, produces bytes in that encoding, and likewise `AsyncStringSerializer`
    and the deserializers.

The later answers:

11. **Null-argument fallback (row 570).** `Create(configs, key, value)` builds an argument that is null from the
    configuration, as Java's three-argument constructors do. A non-null argument wins: its key is ignored, its
    function is not called, and it is never configured.
12. **Async `Configure` returns `ValueTask` (row 571)**, so a serde can wait in it (a schema registry call).
13. **Every constructor of the four real clients is a static `Create` (row 573)**; no public constructor is left.
14. **The asynchronous `Create` returns a `Task` that is awaited (row 574).** It never blocks a thread, awaits with
    `ConfigureAwait(false)`, and takes an optional `CancellationToken` last, which it passes to each `Configure`.
    It is named `Create`, not `CreateAsync`, as no member of an asynchronous client ends in `Async`.
15. **Asynchronous serdes are closed only through `IAsyncDisposable` (row 575).** `Create` on an asynchronous
    client refuses a serde that is `IDisposable` without being `IAsyncDisposable`.

### Java's order, and the binding's

Java's `KafkaProducer(Map<String, Object> configs)` is `this(configs, null, null)`, and
`KafkaProducer(configs, keySerializer, valueSerializer)`:

1. `ProducerConfig.appendSerializerToConfig`: for a null `keySerializer` a missing `key.serializer`, then for a
   null `valueSerializer` a missing `value.serializer`, is a bare `ConfigException`.
2. `new ProducerConfig(…)`: every key is parsed and validated; bare `ConfigException`.
3. Inside the constructor's `try`: a null key serializer is built and
   `configure(config.originals(clientId), true)` is called, then the value serializer with `false`; later the
   rest of the client. Anything thrown is caught, the producer is closed with a zero timeout, and
   `KafkaException("Failed to construct kafka producer", t)` is thrown.

`KafkaConsumer` is the same with `ConsumerConfig.appendDeserializerToConfig` and `Deserializers(config,
keyDeserializer, valueDeserializer, metrics)` inside `AsyncKafkaConsumer`'s `try` ("Failed to construct kafka
consumer").

The binding's order, for all four `Create(configs, key, value)`:

| # | Check | Result |
|---|---|---|
| 1 | `configs` is null | `ArgumentNullException("configs")` |
| 2 | for a null argument, its key is missing or null: the key's, then the value's | bare `ConfigException`, Java's message (Java step 1) |
| 3 | per serde, the key's then the value's: a given one is checked by `SerdeGuard.Check`; a configured one must be a function of the client's interface | bare `IllegalArgumentException` (the guard, as before this phase); bare `ConfigException` (rows 565, 567) |
| 3a | asynchronous clients only: the token is already cancelled | `OperationCanceledException` (row 576) |
| 4 | `ClientConfigs.Prepare`: the other keys' types, a null key | bare `ConfigException` (Java step 2) |
| 5 | the core's config constructor: values, ranges | bare `ConfigException` (Java step 2) |
| 6 | for a null key argument: the key function, the null check, `SerdeGuard.Check`, `Configure(copy of configs, true)`; then the same for a null value argument with `false` | "Failed to construct kafka …" with the cause inside (Java step 3) |
| 6a | asynchronous clients only: the token is read before each function, by each `Configure` it is passed to, and once more before step 7 | `OperationCanceledException`, bare (row 576) |
| 7 | the core's client constructor | "Failed to construct kafka …"; `group.protocol=classic` bare (row 564) |

Steps 6 and 7 are Java's `try`. The functions run between the core's config constructor and its client
constructor, so the whole configuration is validated before any user function runs and no native client exists
yet when one fails: nothing native has to be torn down (row 568).

On an asynchronous client, steps 1 to 5 throw from the call to `Create` itself, before it returns a task; from
step 6 on a failure fails the returned `Task` (row 581).

What is disposed on a failure (row 562), key then value; on an asynchronous client each disposal is awaited:

| Failure | Disposed |
|---|---|
| steps 1–5 | nothing: nothing was built, and a given serde stays its owner's |
| step 6, the key: the function throws or returns null | nothing; a given value stays its owner's, as Java's constructor holds neither yet |
| step 6, the key: the guard refuses the instance or its `Configure` throws | that instance |
| step 6, the value | the instance that failed, if any, and the key, built or given, as Java's constructor holds the key by then and closes it |
| step 7, wrapped | both serdes, given or built, as before this phase (ledger 447) |
| step 7, bare (`group.protocol=classic`) | the built ones; a given one stays its owner's (row 564) |
| a cancellation (asynchronous clients) | the built ones; a given one stays its owner's (row 576) |

### Messages

| Case | Exception | Message |
|---|---|---|
| `configs` is null | `ArgumentNullException` | parameter `configs` |
| a null argument whose key is missing or null | `ConfigException` | `Invalid value null for configuration key.serializer: must be non-null.` (`value.serializer`, `key.deserializer`, `value.deserializer` likewise) |
| wrong kind of value | `ConfigException` | `Invalid value <value> for configuration key.serializer: Expected value to be a System.Func<Confluent.Kafka.Common.Serialization.ISerializer<string>>, but it was a <type of the value>` — `<value>` as `ConfigException` prints any value, both types as `DefaultMethodGuard.TypeText` writes them; `IAsyncSerializer`, `IDeserializer`, `IAsyncDeserializer` on the other clients |
| a bad configuration value | `ConfigException` | the core's, for example `Invalid value hello for configuration request.timeout.ms: Not a number of type INT` |
| function returns null | `KafkaException` | `Failed to construct kafka producer`; `InnerException` is a `ConfigException` `Invalid value null for configuration key.serializer: Expected the function to return a Confluent.Kafka.Common.Serialization.ISerializer<string>, but it returned null` (row 566) |
| function throws, `Configure` throws, `SerdeGuard.Check` refuses a built instance | `KafkaException` | `Failed to construct kafka producer` / `Failed to construct kafka consumer`; `InnerException` is what was thrown |
| a serde on a synchronous client that declares `Close()` without `IDisposable` (unchanged) | `IllegalArgumentException` | `<Type> declares Close() but does not implement IDisposable, so the producer would never call it.` (`consumer` likewise) |
| a serde on an asynchronous client that is `IDisposable` without `IAsyncDisposable` (row 575) | `IllegalArgumentException` | `<Type> implements IDisposable but does not implement IAsyncDisposable, so the producer would never dispose it.` (`consumer` likewise) |
| a serde on an asynchronous client that is not `IAsyncDisposable` and has a public `DisposeAsync()`, `Dispose()` or `Close()` | `IllegalArgumentException` | `<Type> declares DisposeAsync() but does not implement IAsyncDisposable, so the producer would never call it.` (`Dispose()`, `Close()`, `consumer` likewise) |
| `group.protocol=classic` (unchanged) | `UnsupportedOperationException` | `Classic group protocol is not yet supported in this client; set group.protocol=consumer (KIP-848).` |
| the token is cancelled (asynchronous clients) | `OperationCanceledException` | the runtime's |

A guard refusal of a **given** serde is bare (step 3); of a **built** one it is the `InnerException` of "Failed to
construct kafka …" (row 563).

### The mocks

The mocks are unchanged, and each keeps the behaviour it had for a null serde (row 580):

| Mock | A null serde |
|---|---|
| `MockProducer`, `AsyncMockProducer` | `ArgumentNullException("keySerializer")`, then `ArgumentNullException("valueSerializer")` |
| `MockConsumer`, `AsyncMockConsumer` | Java's `ConfigException` "Invalid value null for configuration key.deserializer: must be non-null.", then the value's (`NativeConsumer.RequireDeserializer`) |

Before this phase the real producers already threw Java's `ConfigException` for a null serializer, not an
`ArgumentNullException`; only the mock producers throw the latter. A mock never configures and never disposes a
serde, and has no configuration to read a function from.

### Implementation

All the logic is in internal helpers with a synchronous and an awaited entry; each public `Create` is a thin
wrapper over them.

- `src/Confluent.Kafka/Internal/ConfiguredSerdes.cs` (new). `ConfiguredSerdes<TKeySerde, TValueSerde>` holds the
  pair of one client. Its constructor makes checks 1 to 3. `Build()` and `BuildAwaited(token)` are step 6 and
  return the pair's `SerdeDisposal`; `DisposeBuilt()` and `DisposeBuiltAwaited()` dispose only what was built, for
  a bare failure or a cancellation; `Key` and `Value` give the serdes the client stores. The non-generic
  `ConfiguredSerdes` has four factories (`Serializers`, `AsyncSerializers`, `Deserializers`,
  `AsyncDeserializers`) that name, per kind of client, the two keys, Java's message, the guard and the `Configure`
  extension method.
- `Internal/Interop/ConfigMarshal.cs`: `Validate(prepared, kind)` returns a `ValidatedConfig` (the core's config
  object, step 5), whose `CreateClient()` is step 7. The two native calls existed before; they were made in one
  method.
- `Internal/NativeProducer.cs`, `Internal/NativeConsumer.cs`: `Create(configs, …, serdes)` and
  `CreateAwaited(configs, …, serdes, token)` run steps 4 to 7 and start the client. `CreateAwaited` makes steps 4
  and 5 before its first `await`. The dead `NativeProducer.RequireSerializer` is removed;
  `NativeConsumer.RequireDeserializer` stays for the mock consumers.
- `Internal/SerdeDisposal.cs`: a pair may have a null member, which is skipped; the serdes of an asynchronous
  client (`OfAsyncSerdes`) are disposed only through `IAsyncDisposable`, by `DisposeOnceAsync()` (awaited) or
  `DisposeOnce()` (a synchronous `Dispose()` of the client, which waits on the calling thread, as before).
- `Internal/SerdeGuard.cs`: the check of an asynchronous serde's close (row 575).
- `Internal/ClientConfigs.cs`: the comment on the skipped keys.
- The four client classes: the two `Create` overloads, a private constructor, no public one.
- `Common/Serialization/IAsyncSerializer.cs`, `IAsyncDeserializer.cs` and the four built-in asynchronous serdes
  that have a `Configure`: the `ValueTask` shape. `ISerializer.cs`, `IDeserializer.cs`, `IProducer.cs`,
  `IConsumer.cs`, `IAsyncProducer.cs`, `IAsyncConsumer.cs`: XML docs.
- `analyzers/java-name-baseline.txt`, `analyzers/java-name-allowlist.txt`: the four "missing `(Map)` constructor"
  baseline lines are gone; the allowlist holds, under a comment that cites row 573, the eight Java constructors
  as "missing" and the eight `Create` overloads as "unmatched", and, under a comment that cites rows 571 and 572,
  the `Configure` of the two asynchronous interfaces.

The hot paths (`Send`, `Poll`) do not change: a built serde is stored in the same fields a given one is.

## Steps

One commit per step, `--no-verify`, no `Co-Authored-By`, only files under `dotnet/`. The steps are the ones that
were made; the first version of this plan (S0) had three steps that added config-only constructors, which the
later answers replaced.

- **S0 — plan** (`8e77cecef`). The first version of this file.
- **S1 — `Configure` on the asynchronous serde interfaces returns a `ValueTask`** (`41a85eafc`, row 571).
  `IAsyncSerializerWithConfigure<T>`, `IAsyncDeserializerWithConfigure<T>`, the two extension methods, the four
  built-in asynchronous serdes with a `Configure`, the allowlist lines, tests.
- **S2 — asynchronous serdes are closed only through `IAsyncDisposable`** (`3c84041f5`, row 575). `SerdeGuard`,
  `SerdeDisposal`, the test serdes, the README paragraph on disposal, tests.
- **S3 — `KafkaProducer.Create` and `KafkaConsumer.Create`** (`373c8f843`, rows 553–570, 573).
  `ConfiguredSerdes`, `ConfigMarshal.Validate`, `NativeProducer.Create`, `NativeConsumer.Create`, the two `Create`
  overloads on each synchronous client, `PublicClientCreateTests`, the allowlist lines.
- **S4 — `AsyncKafkaProducer.Create` and `AsyncKafkaConsumer.Create`** (`776d3dd04`, rows 574, 576, 577, 581). The
  awaited entries (`BuildAwaited`, `CreateAwaited`, `DisposeBuiltAwaited`), the two `Create` overloads on each
  asynchronous client, their tests, the allowlist lines.
- **S5 — the public constructors are removed and every caller is moved to `Create`** (`0b1451860`, row 573, row
  582). 101 files: the unit tests, the integration tests, `tests/Confluent.Kafka.GrpcServer.UnitTests`,
  `grpc-server`, `tests/Performance` (`PerformanceCommon`, `PerfV2`, `PerfV3`), `soak/SoakClient`, the six
  examples that build a client, `tools/PackageSmoke`, `README.md`, the XML docs, the analyzer lists, and the test
  that no public constructor remains. The removal and the migration are one commit: with the constructors gone
  no caller of them builds, and with them still public the "no public constructor" test fails.
- **S6 — the broker test** (`b47e75584`). `tests/Confluent.Kafka.IntegrationTests/SerdeConfigCreateBrokerTests.cs`: a
  produce and consume round trip through `Create(configs)` alone, on the synchronous and on the asynchronous
  clients, on the existing Testcontainers fixture; the end-of-phase gates.
- **S7 — this file**, revised to what was done.

## Tests

Unit tests (`tests/Confluent.Kafka.UnitTests`), asserting the exact messages:

- `PublicClientCreateTests` (new), one theory row per client kind (`producer`, `consumer`, `async producer`,
  `async consumer`) unless a name says otherwise:
  - `Create(configs)` calls each function once and configures each serde once, the key (`true`) before the value
    (`false`), with the configuration's entries;
  - with both serdes given: they are used, no function is called, nothing is configured, and no serde key is
    needed;
  - with only the key null, and with only the value null: the null one is built and the given one is taken as it
    is;
  - a null `configs` is `ArgumentNullException("configs")`, before a missing serde;
  - Java's `KafkaProducerTest.testNoSerializerProvided`: a null serde without its key, and with an explicit null,
    is Java's `ConfigException`, the key's first, and before a wrong kind of value;
  - wrong kinds (a string, a `System.Type`, a function of another interface, a function of the other client
    kind's interface): the exact bare `ConfigException`, and no function runs;
  - a bad configuration value is the core's bare `ConfigException`, and no function runs;
  - the key function fails: Java's wrapped failure, and a given value is left alone;
  - the value function fails: Java's wrapped failure, and the key is disposed, built or given;
  - `Configure` throws: Java's wrapped failure, that serde and the key before it are disposed;
  - a built serde the client refuses: Java's wrapped failure around the refusal;
  - a serde with no `Configure` and no dispose is built without either being called;
  - the core fails to build the client: the built serdes are disposed, key then value;
  - the asynchronous `Create`: it throws its checks from the call and reports the rest through its task; it
    returns while a `Configure` is pending and completes only once both are configured; it passes its token to
    `Configure`; a token already cancelled, a cancellation while the key is configured, while the value is
    configured, and once both are configured, each with what is disposed and what is left; a `Configure`
    cancelled by another token is Java's wrapped failure; the built serdes are disposed by the blocking
    `Dispose()` too;
  - a function declared to return the class (`Func<AsyncStringSerializer>`) is accepted and the encoding of the
    configuration is applied (point 10), for the producer and the consumer;
  - `group.protocol=classic` is bare, disposes a built deserializer and leaves a given one;
  - a real client has no public constructor and exactly two public static `Create` overloads with Java's
    parameter names; a mock keeps its public constructors and has no `Create`.
- `AsyncSerializationTests`, `AsyncProducerAwaitedSerializerTests` (S1): the `ValueTask` `Configure` of the
  interfaces, the extension methods and the built-in serdes.
- `AsyncSerdeGuardTests`, `AsyncDeserializerDisposalTests`, `AsyncProducerAwaitedSerializerTests` (S2): the
  refusal of an `IDisposable`-only serde with its message, and disposal only through `IAsyncDisposable`.
- `AsyncSerdeDisposalOnABlockedThreadTests` (S5): a `Create` whose client the core fails to build fails its task
  and disposes each serde once. Before S5 these two tests ran a failing constructor on a thread with a blocked
  synchronization context; an awaited `Create` blocks no thread, so they are plain awaited tests.

The tests reuse the existing fixtures (`UnreachableProducers`, `TestTimeout`, the disposal test serdes) and build
real clients against an unreachable bootstrap address, as the existing tests do. No test of this phase uses
`Task.Run` or waits synchronously on a task.

Integration tests (`tests/Confluent.Kafka.IntegrationTests`, Docker):
`SerdeConfigCreateBrokerTests.KafkaProducerAndKafkaConsumer_BuiltFromTheirConfigurationAlone_RoundTripARecord` and
`AsyncKafkaProducerAndAsyncKafkaConsumer_BuiltFromTheirConfigurationAlone_RoundTripARecord`. Each of the four
functions is called once; the key travels as UTF-16LE, which both sides read from the configuration; the value's
bytes are consumed through a byte-array deserializer and are the UTF-16BE bytes the value's serializer was
configured to write.

## Verification

Commands, from `dotnet/`:

- `dotnet build -c Release -p:CargoProfileDir=debug` (the solution; the library also for net462 and
  netstandard2.0, the unit tests also for net462): 0 warnings, 0 errors, CKRDA0013 / CKRDA0014 and the trimming
  and AOT analyzers included; no new `[UnconditionalSuppressMessage]`.
- `dotnet test tests/Confluent.Kafka.UnitTests -c Release -p:CargoProfileDir=debug -f net8.0 --no-build`, and
  `-f net10.0`.
- `dotnet test analyzers/Confluent.Kafka.Analyzers.UnitTests -c Release -p:CargoProfileDir=debug --no-build`.
- `dotnet test tests/Confluent.Kafka.IntegrationTests -c Release -p:CargoProfileDir=debug -f net8.0 --no-build`,
  and `-f net10.0`.
- `dotnet test tests/Confluent.Kafka.GrpcServer.UnitTests -c Release -p:CargoProfileDir=debug`.
- `dotnet test soak/SoakClient.Tests -c Release -p:CargoProfileDir=debug`.
- `dotnet format --verify-no-changes` (the solution), and the same on each project outside the solution that S5
  touched.

## Gates per step

Counts are passed / skipped / failed per framework, the same on net8.0 and net10.0 unless stated. The baseline
before this phase is 9,526 / 51 / 0 unit tests, 245 analyzer tests and 29 integration tests per framework.

| Step | Build | Unit tests | Analyzer tests | Other | Format |
|---|---|---|---|---|---|
| S0 | — | — | — | — | — |
| S1 | 0 warnings, 0 errors | 9,528 / 51 / 0 | 245 | — | exit 0 |
| S2 | 0 warnings, 0 errors | 9,538 / 51 / 0 | 245 | — | exit 0 |
| S3 | 0 warnings, 0 errors | 9,571 / 51 / 0 | 245 | — | exit 0 |
| S4 | 0 warnings, 0 errors | 9,622 / 51 / 0 | 245 | — | exit 0 |
| S5 | 0 warnings, 0 errors | 9,628 / 51 / 0 | 245 | gRPC server tests 228 (net10.0); `SoakClient.Tests` 201; `PerfV2`, `PerfV3`, `Confluent.Kafka.PerformanceTests` and the nine examples build with 0 warnings, 0 errors | exit 0, and exit 0 on the twelve projects outside the solution |
| S6 | 0 warnings, 0 errors | 9,628 / 51 / 0 (net10.0: one unrelated thread-timing test failed once, 9,628 / 51 / 0 on two reruns; open question 7) | 245 | integration tests 31 / 0 / 0 (29 and the two new ones); gRPC server tests 228 (net10.0); `SoakClient.Tests` 201 | exit 0 |
| S7 | — (this file only) | — | — | — | — |

Not run on the development host:

- net462: the library and the unit tests build for it; no test ran on it (row 569).
- `tools/PackageSmoke`: it builds only against a packed package (`-p:ConfluentKafkaVersion=`), so its source was
  edited and not compiled (row 583).

## Out of scope

- `KafkaProducer(Properties)` / `KafkaConsumer(Properties)`: the binding has no `Properties` form (§4 **Config**).
- The mocks.
- The other settings Java fills by reflection (`partitioner.class`, `interceptor.classes`, `metric.reporters`, …).
- Any FFI change, the resolved `client.id` included (row 561).
- A function that returns a `Task`: the function is synchronous on every client; a serde that needs asynchronous
  setup does it in its awaited `Configure`.
- `STATUS.md` and the archive of the comment files: the Manager's close-out.

## Ledger rows

Rows 553–560 are the owner's first decision of 2026-11-19; where a later answer changed the shape, the row says
which. Rows 570, 571, 573, 574 and 575 are the owner's later answers of the same day, given through the Manager.
The other rows are this plan's own choices and deviations, for the owner to accept or change.

| # | Source | Topic | Decision | Status |
|---|---|---|---|---|
| 553 | Owner 2026-11-19 | The configuration route | the four real clients can be built from the configuration alone, Java's `KafkaProducer(Map)` / `KafkaConsumer(Map)`; reverses the "constructor arguments only" part of ledger 365 for serializers and deserializers. Shape amended by row 573: the route is `Create(configs)`, not a constructor | Agreed |
| 554 | Owner 2026-11-19 | Value of `key.serializer` and its peers | a parameterless function returning the serde, tested with `is Func<I…<T>>` (delegate covariance); no reflection, no `System.Type`, no class name; any other value is a `ConfigException` naming the key and what is expected | Agreed |
| 555 | Owner 2026-11-19 | Missing or null key | Java's bare `ConfigException` "Invalid value null for configuration key.serializer: must be non-null.", the key before the value, as the first check | Agreed |
| 556 | Owner 2026-11-19 | `Configure` | called iff the built instance implements the `…WithConfigure<T>` interface, key (`true`) then value (`false`), once each; a serde given as an argument is never configured. Amended by rows 571 and 574: on an asynchronous client it is awaited | Agreed |
| 557 | Owner 2026-11-19 | Failure while building | the function returns null or throws, or `Configure` throws: `KafkaException` "Failed to construct kafka producer" / "… consumer" with the cause inside; what was built is disposed | Agreed |
| 558 | Owner 2026-11-19 | Order | the binding's configuration checks and the core's validation come first, bare, before the functions run | Agreed |
| 559 | Owner 2026-11-19 | Disposal and guard | a built serde is disposed at close as a given one, and `SerdeGuard.Check` applies to it | Agreed |
| 560 | Owner 2026-11-19 | Unchanged | the five own-role keys never reach the core; the mocks do not change. Amended by rows 570 and 573: the three-argument form is `Create(configs, key, value)` and builds a null argument from the configuration | Agreed |
| 561 | Actor 130 | The `configs` given to `Configure` | a fresh copy of the user's configuration for each call, serde keys included, as Java's `originals()` copies; it does not carry the `client.id` the core resolves, which Java adds (`config.originals(singletonMap(CLIENT_ID_CONFIG, clientId))`). Why: no FFI function returns the resolved id. Goes when: the C ABI returns the client id of a producer and a consumer | Proposed |
| 562 | Actor 130 | Disposal on a failed `Create` | the table of "Java's order, and the binding's": a built instance the guard refuses or whose `Configure` throws is disposed; a value failure also disposes the key, built or given; a wrapped failure of the core disposes both, given or built; a bare one and a cancellation dispose only the built ones. Java closes less in one case: a serializer whose `configure` throws is not yet assigned and is not closed. Why: the user holds no reference to a built serde, so nobody else can dispose it | Proposed |
| 563 | Actor 130 | `SerdeGuard.Check` on a built serde | its `IllegalArgumentException` is the `InnerException` of "Failed to construct kafka …", where a given serde's is thrown bare. Why: the instance exists only inside Java's `try` | Proposed |
| 564 | Actor 130 | A bare failure of the core's client constructor | the consumer's `group.protocol=classic` refusal (`UnsupportedOperationException`) comes after the functions ran; it stays bare and the built deserializers are disposed, where ledger 447 leaves given ones alone on a bare error. Why: the binding repeats no core check to refuse it earlier. Goes when: the core's config constructor refuses it | Proposed |
| 565 | Actor 130 | A value of the wrong kind | a bare `ConfigException` with the binding's own text "Expected value to be a System.Func<…>, but it was a …". Java has no such case: a class name it cannot load is a bare `ConfigException` "Class … could not be found.", and a class of another interface is a wrapped `KafkaException` "… is not an instance of …" | Proposed |
| 566 | Actor 130 | A function that returns null | the inner exception is a `ConfigException` for the key with the value `null`: "Expected the function to return a …, but it returned null". Java cannot have the case | Proposed |
| 567 | Actor 130 | Position of the kind check | after Java's missing-key check and before the other keys' type checks; Java's place for a CLASS type error is its `ConfigDef` order, which no other binding check follows either | Proposed |
| 568 | Actor 130 | Where the functions run | between the core's config constructor (`ConfigMarshal.Validate`) and its client constructor (`ValidatedConfig.CreateClient()`); no new FFI entry point, and no native client exists when a function fails | Proposed |
| 569 | Actor 130 | net462 | the library and the unit tests build for net462; no test of this phase ran on it on the development host | Open |
| 570 | Owner 2026-11-19, through the Manager | Null-argument fallback | `Create(configs, key, value)` builds a null argument from its configuration key, as Java's three-argument constructors do; a non-null argument wins, its key is not read and it is never configured. Before this phase a null argument was always Java's "must be non-null" `ConfigException` | Agreed |
| 571 | Owner 2026-11-19, through the Manager | Asynchronous `Configure` | `IAsyncSerializerWithConfigure<T>.Configure` and `IAsyncDeserializerWithConfigure<T>.Configure` return a `ValueTask`, and so do the two extension methods. Supersedes P7 ledger 546's "the shape of the sync ones" | Agreed |
| 572 | Actor 130 | The token of the asynchronous `Configure` | a required `CancellationToken cancellationToken` last on the interface method, since an interface member an implementer writes takes no default; optional (`= default`) on the extension method the user calls, as §3 Java names rule 7 has it for every awaited public method | Proposed |
| 573 | Owner 2026-11-19, through the Manager | `Create` | every constructor of `KafkaProducer`, `AsyncKafkaProducer`, `KafkaConsumer` and `AsyncKafkaConsumer` is a static `Create` with the constructor's parameters; no public constructor is left; the mocks keep theirs. Deviation from Java's constructors, as `Admin.Create` is Java's own factory | Agreed |
| 574 | Owner 2026-11-19, through the Manager | The asynchronous `Create` | returns `Task<AsyncKafka…>` and is awaited; it awaits each `Configure` and blocks no thread; named `Create`, with an optional `CancellationToken` last | Agreed |
| 575 | Owner 2026-11-19, through the Manager | Closing an asynchronous serde | only through `IAsyncDisposable`. A serde that is `IDisposable` and not `IAsyncDisposable` is refused with "<Type> implements IDisposable but does not implement IAsyncDisposable, so the producer would never dispose it."; a public `Dispose()` on a serde that is `IAsyncDisposable` is accepted and never called; a failed `Create` awaits the disposal. Supersedes, in P7 ledger 548, "else `IDisposable`" and "a failed constructor waits on the calling thread". What stays of 548: once, on every path; a synchronous `Dispose()` of the client waits on the calling thread; a serde's `DisposeAsync()` is started with no synchronization context and the default task scheduler | Agreed |
| 576 | Actor 130 | A cancelled `Create` | a bare `OperationCanceledException` carrying the token, never wrapped; the serdes `Create` built are disposed, a given one is left to its owner; the token is read after checks 1–3, before each function, by each `Configure`, and before the core builds the client, and not once the client exists. A `Configure` that throws `OperationCanceledException` while `Create`'s own token is not cancelled is an ordinary failure, wrapped as Java's | Proposed |
| 577 | Actor 130 | Where `Configure` runs | on the calling flow, with the caller's synchronization context, up to its first `await`; the library awaits it with `ConfigureAwait(false)`. Unlike a serde's `DisposeAsync()` (P7 ledger 548), it is not started with the context removed: `Create` is awaited, so nothing waits on a blocked thread | Proposed |
| 578 | Actor 130 | The overloads and the token | no call is ambiguous: `Create(configs, null, null)` binds the three-argument overload, `Create(configs, token)` the config-only one. The optional token is rule 7's, the only optional parameter | Proposed |
| 579 | Actor 130 | Order of checks against Java's | three differences, each with its own row: the guard of a given serde runs before the configuration is validated (as before this phase); the kind check of a serde key runs before the other keys' type checks (567); the `group.protocol=classic` refusal comes after the serdes are built (564). On an asynchronous client a token already cancelled is seen after checks 1–3 and before checks 4–5 (576) | Proposed |
| 580 | Actor 130 | The mocks' null serde | unchanged per mock: `MockProducer` and `AsyncMockProducer` throw `ArgumentNullException`; `MockConsumer` and `AsyncMockConsumer` throw Java's `ConfigException`. No mock has a `Create` or reads a function | Proposed |
| 581 | Actor 130 | Where the asynchronous `Create` fails | checks 1–5 throw from the call itself; from the first function on, a failure fails the returned `Task`. Why: the checks are argument and configuration errors, known before anything waits, as an awaited method's argument checks are | Proposed |
| 582 | Actor 130 | Callers outside the library | moving them to an awaited `Create` changed some shapes: the gRPC server's chaos factories return a `Task`; `ITransactionalFactory.CreateProducer` (soak client) returns `ValueTask<ITransactionalProducer>`; the PerfV3 asynchronous backends have a private constructor and a `Create`; the two "failed construction on a blocked thread" unit tests are plain awaited tests | Proposed |
| 583 | Actor 130 | `tools/PackageSmoke` | its source is moved to `Create`; it builds only against a packed package, so it was not compiled on the development host | Open |

### P7 rows this phase supersedes

P7's plan file is not edited; these rows are the record.

| P7 row | P7 text | Superseded by |
|---|---|---|
| 546 | "public `AsyncSerializerExtensions.Configure` / `AsyncDeserializerExtensions.Configure`, the shape of the sync ones" | row 571: they return a `ValueTask` and take a `CancellationToken` (572). The rest of 546 stands: a class implementing a synchronous and an asynchronous interface must qualify the call |
| 548 | "`IAsyncDisposable` first, else `IDisposable`, once, on every path; a synchronous `Dispose()` and a failed constructor wait on the calling thread" | row 575, in part: no `IDisposable` fallback, and a failed `Create` awaits. The rest of 548 stands |

## Draft rule text (not applied)

No rule file was edited. Each section gives the file and line, the sentence that becomes stale, and its
replacement. The line numbers are those of `dotnet/CLAUDE.md` at this branch's head. P7's own drafts R1–R5 (its
plan file) are not applied either, so the rule text still describes asynchronous serdes as deferred; the drafts
below are written against the text as it stands and name P7's draft where they add to it.

### R1 — `dotnet/CLAUDE.md:958`, §4 status table, row **Serializers**: "the client … never calls `Configure`"

Stale: "The headers forms and `Configure` follow §3 Java names rule 10; the client calls the headers forms and never
calls `Configure`, as Java never configures a given instance."

Replacement: "The headers forms and `Configure` follow §3 Java names rule 10; the client calls the headers forms.
It calls `Configure` only on a serde it built from the configuration (**From the configuration**, below), never on
one it was given, as Java never configures a given instance."

### R2 — `dotnet/CLAUDE.md:958`, the same row: "**Constructor arguments only** (ledger 365) … The Rust core makes the same choice.)*"

Stale: the passage from "**Constructor arguments only** (ledger 365): Java's config-only constructors
`KafkaProducer(Map)`, `KafkaProducer(Properties)`, `KafkaConsumer(Map)` and `KafkaConsumer(Properties)` are not
translated, and nothing is built by reflection" to "The Rust core makes the same choice.)*", its reason "a C#
constructor cannot be async" included.

Replacement: "**From the configuration** (M23/P8, ledgers 553–581): each constructor of the four real clients is a
static `Create` with the constructor's parameters, and the class has no public constructor: `Create(configs)` and
`Create(configs, keySerializer, valueSerializer)` (`Map`; there is no `Properties` form, §4 **Config**).
`KafkaProducer.Create` and `KafkaConsumer.Create` return the client; `AsyncKafkaProducer.Create` and
`AsyncKafkaConsumer.Create` return a `Task` of it and take a last optional `CancellationToken` *(deviation from
Java's constructors, ledger 573. Why: a C# constructor cannot await, and an asynchronous serde's `Configure` may
wait; the synchronous clients have the same shape so that the four are built alike)*. As in Java, a serializer
argument that is null is built from the configuration: under `key.serializer` / `value.serializer` (consumer:
`key.deserializer` / `value.deserializer`) the configuration holds a **function that returns the serde**, a
`Func<ISerializer<TKey>>` on `KafkaProducer`, a `Func<IAsyncSerializer<TKey>>` on `AsyncKafkaProducer`, and the
deserializer interfaces on the consumers; a delegate returning a class that implements the interface is accepted,
as `Func<out T>` is covariant. Nothing is built by reflection: a string, a `System.Type` or a function of another
interface is a bare `ConfigException` "Expected value to be a System.Func<…>, but it was a …". The order is
Java's: a null argument whose key is missing or null (the key, then the value) is Java's "Invalid value null for
configuration key.serializer: must be non-null."; then the binding's type checks and the core's validation of the
configuration, all bare; then, as inside Java's constructor `try`, the key function runs and the client calls
`Configure(configs, true)` on the instance if it implements the `…WithConfigure<T>` interface, then the value with
`false`, and the core builds the client. On an asynchronous client `Configure` returns a `ValueTask` and is
awaited with `Create`'s token; the checks before the first function throw from the call, and what follows fails
the task (ledger 581). `configs` is a copy of the user's configuration *(deviation: without the `client.id` the
core resolves, which Java adds. Goes when: the C ABI returns it, ledger 561)*. A function that returns null or
throws, a `Configure` that throws, or a `SerdeGuard` refusal of a built instance is a `KafkaException` "Failed to
construct kafka producer" / "… consumer" with the cause as `InnerException`; a cancelled `Create` is a bare
`OperationCanceledException` (ledger 576). A serde given as an argument is never configured. The mocks keep their
public constructors, have no `Create` and read no function, as Java's mocks have no configuration route. Each
other setting Java fills by reflection (`partitioner.class`, `interceptor.classes`, `metric.reporters`,
`config.providers`, the SASL and OAuth handler classes, `ssl.engine.factory.class`) gets, when it is translated, a
setter on `ProducerConfig` / `ConsumerConfig` that takes an object the user built, as the Rust core's
`ProducerConfig::set_partitioner` does. *(Deviation from Java: a function where Java takes a class or a class
name. Why: without reflection, trimming and native AOT stay open, and the user's function can pass constructor
arguments.)*"

### R3 — `dotnet/CLAUDE.md:958`, the same row: "**Disposal** (M22/P2) …" and "An `IAsyncDisposable`-only serde is not disposed."

Stale: "the four real clients dispose each serializer or deserializer they were given that is `IDisposable`, once,
key then value, after the destroy (§4 **Close/Dispose**, step 6), and when the core fails to build the client with
Java's wrapped "Failed to construct kafka producer" / "… consumer", not for an error it returns bare, such as a
configuration value it cannot parse, which Java raises before its constructor's `try` (ledger 447)" and "An
`IAsyncDisposable`-only serde is not disposed."

Replacement: "the four real clients dispose each serializer or deserializer they were given, or built from the
configuration, once, key then value, after the destroy (§4 **Close/Dispose**, step 6): a synchronous client one
that is `IDisposable`, an asynchronous client one that is `IAsyncDisposable`, and only through that interface
(ledger 575). An asynchronous client refuses, at `Create`, a serde that is `IDisposable` without being
`IAsyncDisposable` ("<Type> implements IDisposable but does not implement IAsyncDisposable, so the producer would
never dispose it."). They are also disposed when the core fails to build the client with Java's wrapped "Failed
to construct kafka producer" / "… consumer", not, for a given one, for an error it returns bare, such as a
configuration value it cannot parse, which Java raises before its constructor's `try` (ledger 447). A serde the
client built is disposed on every failure of `Create` after its function returned it, the bare
`group.protocol=classic` refusal and a cancellation included, since the user holds no reference to it (ledgers
562, 564, 576)." P7 has no draft for this sentence: its ledger 548
changed the behaviour ("`IAsyncDisposable` first, else `IDisposable`") without a rule draft, so this one covers
both phases.

### R4 — `dotnet/CLAUDE.md:958`, the same row, status cell

Stale: "✅ M22/P2 (constructor arguments only; disposal)".

Replacement: "✅ M22/P2 (disposal); M23/P8 (`Create`, serdes from the configuration)".

### R5 — `dotnet/CLAUDE.md:959`, §4 status table, row **Config**: "on each `Kafka*` client constructor" and "nothing reads them"

Stale: "One `IReadOnlyDictionary<string, object?> configs` on each `Kafka*` client constructor" and "The five
own-role serializer keys (`key.serializer`, `value.serializer`, `serializer.encoding`, `key.serializer.encoding`,
`value.serializer.encoding`; consumer: the deserializer ones) are neither checked nor sent, as in Python; nothing
reads them, since a serializer is a constructor argument only (§4 **Serializers**, ledger 365)."

Replacement: "One `IReadOnlyDictionary<string, object?> configs` on each `Kafka*` client's `Create`" and "The five
own-role serializer keys (`key.serializer`, `value.serializer`, `serializer.encoding`, `key.serializer.encoding`,
`value.serializer.encoding`; consumer: the deserializer ones) are never sent to the core, as in Python. `Create`
reads `key.serializer` / `value.serializer` as a function for a serializer argument that is null, and hands the
whole configuration, the encoding keys included, to the `Configure` of each serde it builds (§4 **Serializers**,
ledgers 553, 570); for a serializer that is given, its key is not read."

### R6 — `dotnet/CLAUDE.md:959`, the same row: "a `System.Type` for CLASS"

Stale: "(… an `IList` for LIST; a `System.Type` for CLASS)". The sentence stays true for the CLASS keys the type
check covers; it needs one addition so it is not read as covering the serde keys.

Replacement: "(… an `IList` for LIST; a `System.Type` for CLASS, the client's own serializer and deserializer keys
excepted, whose value is a function, §4 **Serializers**)".

### R7 — `dotnet/CLAUDE.md:837-841`, the bullet "**Consumer deserializers.**"

Stale: "The config takes no classes, so the consumer has no `key.deserializer` / `value.deserializer` route: a null
deserializer throws Java's `ConfigException` "Invalid value null for configuration key.deserializer: must be
non-null." (the value likewise), on all four consumers, the mocks included *(Java's `MockConsumer` takes no
deserializers)*."

Replacement: "The config takes no classes. On `KafkaConsumer` and `AsyncKafkaConsumer` a deserializer is an
argument of `Create` or, for an argument that is null, a function under `key.deserializer` / `value.deserializer`
(§4 **Serializers**); a null argument whose key is missing or null throws Java's `ConfigException` "Invalid value
null for configuration key.deserializer: must be non-null." (the value likewise). The mock consumers have no
configuration, so a null deserializer always throws it *(Java's `MockConsumer` takes no deserializers)*."

### R8 — `dotnet/CLAUDE.md:330-334` and `:532-537`, the §3 sketches of the real clients

Stale: "// Real clients take the serializers — Java KafkaProducer(Map, Serializer<K>, Serializer<V>). Java's (Map)
and // (Properties) constructors are not translated, and a given serializer that is IDisposable is disposed at the
// producer's teardown (§4 Serializers, Close/Dispose)." with `public AsyncKafkaProducer(IReadOnlyDictionary<string,
object?> configs, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer);`, and "// 3-param ctor
(decision A) — Java KafkaConsumer(Map, Deserializer<K>, Deserializer<V>), KafkaConsumer.java:609. // Java's (Map)
and (Properties) constructors are not translated (§4 Serializers)." with `public
AsyncKafkaConsumer(IReadOnlyDictionary<string, object?> configs, IDeserializer<TKey> keyDeserializer,
IDeserializer<TValue> valueDeserializer);`.

Replacement (the serde interfaces are the asynchronous ones, which the asynchronous clients take since M23/P7,
ledger 536; the sketch still shows the synchronous ones and P7 has no draft for it):

```csharp
// Real clients are built by Create — Java KafkaProducer(Map) and KafkaProducer(Map, Serializer<K>, Serializer<V>);
// there is no public constructor and no (Properties) form. A null serializer is built from the function under its
// configuration key and configured; each serializer is disposed at the producer's teardown (§4 Serializers).
public sealed class AsyncKafkaProducer<TKey, TValue> : IAsyncProducer<TKey, TValue> {
    public static Task<AsyncKafkaProducer<TKey, TValue>> Create(
        IReadOnlyDictionary<string, object?> configs, CancellationToken cancellationToken = default);
    public static Task<AsyncKafkaProducer<TKey, TValue>> Create(IReadOnlyDictionary<string, object?> configs,
        IAsyncSerializer<TKey>? keySerializer, IAsyncSerializer<TValue>? valueSerializer,
        CancellationToken cancellationToken = default);   // typed values, §4 Config
}

public sealed class AsyncKafkaConsumer<TKey, TValue> : IAsyncConsumer<TKey, TValue> {   // Java `KafkaConsumer` (KIP-848)
    // Java KafkaConsumer(Map) and KafkaConsumer(Map, Deserializer<K>, Deserializer<V>); no public constructor.
    public static Task<AsyncKafkaConsumer<TKey, TValue>> Create(
        IReadOnlyDictionary<string, object?> configs, CancellationToken cancellationToken = default);
    public static Task<AsyncKafkaConsumer<TKey, TValue>> Create(IReadOnlyDictionary<string, object?> configs,
        IAsyncDeserializer<TKey>? keyDeserializer, IAsyncDeserializer<TValue>? valueDeserializer,
        CancellationToken cancellationToken = default);   // typed values, §4 Config
}
```

### R9 — `dotnet/CLAUDE.md:780`, §3 Java-names table, row "`Map<String, Object>` / `Properties` (a client's configuration)"

Stale: "one `IReadOnlyDictionary<string, object?> configs` per `Kafka*` constructor".

Replacement: "one `IReadOnlyDictionary<string, object?> configs` per `Kafka*` `Create`".

### R10 — `dotnet/CLAUDE.md:897-902`, §3 Java names rule 8

Stale: nothing in the rule is wrong, but "Java's constructor and method overloads, each in Java's parameter order"
has no exception for the four real clients.

Addition, after "a struct or sealed class has none.": "The constructors of the four real clients (`KafkaProducer`,
`AsyncKafkaProducer`, `KafkaConsumer`, `AsyncKafkaConsumer`) are static `Create` methods with the constructor's
parameters in Java's order, and the classes have no public constructor *(deviation, ledger 573. Why: building a
client may await a serde's `Configure`, which a constructor cannot)*; the asynchronous ones return a `Task` and
take rule 7's token."

### R11 — `dotnet/CLAUDE.md:730`, §3, the bullet "**Async serde deferred**" (P7's draft R2)

The rule text has no sentence on the asynchronous serdes' `Configure` or close: the bullet at `:730` still says
they are deferred, and P7's draft R2, which replaces it, describes the one method of `IAsyncSerializer<T>` /
`IAsyncDeserializer<T>` and nothing else.

Addition to P7's draft R2, when it is applied: "A configurable one implements
`IAsyncSerializerWithConfigure<T>` / `IAsyncDeserializerWithConfigure<T>`, whose `Configure` returns a
`ValueTask` and takes a last `CancellationToken`, required on the interface method and optional on the extension
method (`AsyncSerializerExtensions.Configure`, `AsyncDeserializerExtensions.Configure`; ledgers 571, 572); the
asynchronous `Create` awaits it on a serde it builds (§4 **Serializers**). An asynchronous serde is closed only
through `IAsyncDisposable` (ledger 575)."

### R12 — `dotnet/README.md` (not a rule file; edited in S2 and S5)

The serialization section now says that a client is built by `Create`, shows the configuration route for a
synchronous and an asynchronous client, and says that an asynchronous serde is disposed only through
`IAsyncDisposable`. Recorded here so the owner reads it with the rule drafts.

## Open questions

1. Rows 561–568, 572 and 576–582 are "Proposed": each is the Actor's choice and waits for the owner.
2. Row 581: should the asynchronous `Create` report its upfront checks (a null `configs`, a missing key, a wrong
   kind of value, a bad configuration value) through its `Task` instead of throwing them from the call? The
   implementation throws from the call; the logic is in internal helpers, so the other shape is a change to the
   four thin wrappers and their tests.
3. Row 575: a public `Dispose()` on a serde that is `IAsyncDisposable` was refused before S2 (as never called) and
   is accepted now, since such a class commonly implements both patterns. Is that the intended reading of "closed
   only through `IAsyncDisposable`"?
4. Row 561: `Configure` does not receive the resolved `client.id`, which Java's serdes get. It needs a C ABI
   accessor, which is outside this phase.
5. Rows 569 and 583: the net462 tests and `tools/PackageSmoke` were not run on the development host.
6. The rule files still lack P7's drafts R1–R5; this plan's draft R11 is an addition to P7's draft R2.
7. One unit test outside this phase,
   `PublicMockConsumerOwnerWaitTests.AsyncMock_ACallWaitingForAnotherThreadsUse_IsWokenAndRefused_WhenThatThreadsRebalanceBegins`,
   failed once on net10.0 during the S6 gates (9,627 passed, 1 failed) and passed on net8.0, in six reruns of its
   class and in two reruns of the whole net10.0 suite (9,628 / 51 / 0 each). It is a thread-timing test of
   `AsyncMockConsumer`, which this phase does not touch.
