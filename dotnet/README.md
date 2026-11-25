# Confluent.Kafka

Confluent's .NET client for Apache Kafka. All the Kafka logic, from batching and partitioning to
retries and the group protocol, runs in the Confluent Kafka Rust client, which this package ships
as a native library. Its API follows the Apache Kafka Java client: `KafkaProducer`, `KafkaConsumer`,
`ProducerRecord`, `ConsumerRecord`, futures for sends, and so on. Each client has a blocking form
and an async form. Each type is in the namespace of its Java package, without `org.apache.kafka`
and `clients`: `KafkaProducer` is in `Confluent.Kafka.Producer`, `Serdes` in
`Confluent.Kafka.Common.Serialization`.

> This is a pre-release of a new major version. Its API is not the API of Confluent.Kafka 2.x
> (the librdkafka-based client). The two share the package id, so a project gets one of them for
> its whole dependency graph. A package built on Confluent.Kafka 2.x, such as the
> Confluent.SchemaRegistry.Serdes packages, still restores beside this version, but it does not
> work with it.

## Platforms

- .NET 10, and .NET Framework 4.6.2 or later on Windows
- Linux x64 and arm64, with glibc 2.28 or later (RHEL 8, Debian 10, Ubuntu 20.04 and later)
- macOS arm64 (Apple silicon): the native library needs macOS 11 or later, and .NET 10 has its
  own [macOS minimum](https://github.com/dotnet/core/blob/main/release-notes/10.0/supported-os.md)
- Windows x64 and x86, Windows 10 or Windows Server 2016 or later. The native library links the
  C runtime statically, so it needs no Visual C++ Redistributable.

Other .NET runtimes on these platforms, such as .NET 8, get the package's .NET Standard 2.0
assembly. An application on them builds and runs, Native AOT included, but they are not supported:
.NET 8 is out of Microsoft support. That assembly refuses a `Half`, `Int128` or `UInt128`
configuration value, types .NET Standard 2.0 does not have.

On .NET Framework, NuGet copies the native library only into a project that has a runtime
identifier. An SDK-style application gets one from its platform target, `win-x86` for AnyCPU and
x86, `win-x64` for x64. Any other project, such as a test project, sets one:
`<RuntimeIdentifier>win-x64</RuntimeIdentifier>`, or `win-x86` for a 32-bit process.

macOS x64 (Intel), Windows arm64 and musl-based Linux (Alpine) are not supported yet. On them the
package installs and a project builds, but creating a client, a mock one included, throws
`DllNotFoundException`: the package has no native library for macOS x64 or Windows arm64, and its
Linux libraries need glibc.

## Producing

```csharp
using System.Collections.Generic;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;

var config = new Dictionary<string, object?>
{
    ["bootstrap.servers"] = "localhost:9092",
    ["linger.ms"] = 5,
};
using var producer = KafkaProducer<string, string>.Create(config, new StringSerializer(), new StringSerializer());
var record = new ProducerRecord<string, string>("orders", "key", "value");
RecordMetadata metadata = producer.Send(record).Get();
```

## Consuming

```csharp
using System;
using System.Collections.Generic;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

var config = new Dictionary<string, object?>
{
    ["bootstrap.servers"] = "localhost:9092",
    ["group.id"] = "orders-reader",
    ["group.protocol"] = "consumer",
};
using var consumer = KafkaConsumer<string, string>.Create(config, new StringDeserializer(), new StringDeserializer());
consumer.Subscribe(new[] { "orders" });
foreach (var record in consumer.Poll(TimeSpan.FromSeconds(1)))
{
    Console.WriteLine($"{record.Key()}: {record.Value()}");
}
```

The consumer supports the consumer group protocol of KIP-848 only, so `group.protocol` must be
`consumer`.

## Serializers and deserializers

A producer or consumer is built by its static `Create`, not by a constructor; the mocks keep their
constructors. `Create` on `AsyncKafkaProducer` and `AsyncKafkaConsumer` returns a `Task` to await,
and takes a `CancellationToken` last. A client takes one serializer or deserializer for keys and one
for values, either as arguments of `Create` or from its configuration.
`Confluent.Kafka.Common.Serialization` has built-in ones for strings, byte arrays,
`ReadOnlyMemory<byte>?`, booleans, the integer and floating-point types and `Guid`, named as in Java
(`StringSerializer`, `LongDeserializer`, `UUIDSerializer`).

### Serializers from the configuration

`Create(configs)`, and `Create(configs, key, value)` for an argument that is `null`, reads the
serializer from `key.serializer` or `value.serializer` (`key.deserializer`, `value.deserializer` on a
consumer). Where Java's configuration holds a class, this one holds a function that returns the
instance:

```csharp
using System;
using System.Collections.Generic;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;

var config = new Dictionary<string, object?>
{
    ["bootstrap.servers"] = "localhost:9092",
    ["key.serializer"] = new Func<ISerializer<string>>(() => new StringSerializer()),
    ["value.serializer"] = new Func<ISerializer<string>>(() => new StringSerializer()),
    ["value.serializer.encoding"] = "UTF-16",
};
using var producer = KafkaProducer<string, string>.Create(config);
```

The client calls each function once, the key's first, and then calls `Configure(configs, isKey)` on a
serializer that implements `ISerializerWithConfigure<T>` (`IDeserializerWithConfigure<T>`), as Java
configures a serializer it builds. A serializer given as an argument wins over the configuration and
is never configured, as in Java. The function is a `Func<ISerializer<T>>` on a synchronous client and
a `Func<IAsyncSerializer<T>>` on an asynchronous one, whose `Configure` returns a `ValueTask` that
`Create` awaits; a function declared to return the class itself (`Func<StringSerializer>`) is
accepted too. A `null` argument whose key is missing fails with Java's `ConfigException`
("Invalid value null for configuration key.serializer: must be non-null."). A function that returns
`null` or throws, or a `Configure` that throws, fails `Create` with the `KafkaException` "Failed to
construct kafka producer" (or "consumer") around that failure. The client disposes the serializers it
built, as it disposes the given ones.

### Synchronous and asynchronous clients

The synchronous clients (`KafkaProducer`, `KafkaConsumer` and their mocks) take `ISerializer<T>` and
`IDeserializer<T>`. The asynchronous clients (`AsyncKafkaProducer`, `AsyncKafkaConsumer` and their
mocks) take only `IAsyncSerializer<T>` and `IAsyncDeserializer<T>`, whose method returns a
`ValueTask` and receives the `CancellationToken` of the `Send` or `Poll` it serves. As with the
synchronous interfaces, a serializer or deserializer that reads the record's headers implements
`IAsyncSerializerWithHeaders<T>` or `IAsyncDeserializerWithHeaders<T>`, and the client then calls
its headers form; a class that declares a headers form without that interface is refused when the
client is built, since the client would never call it. Each built-in has an asynchronous class, `Async` and its name
(`AsyncStringSerializer`, `AsyncLongDeserializer`); a client given two of them serializes or
deserializes inside the call and awaits nothing.

```csharp
using System.Collections.Generic;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;

var config = new Dictionary<string, object?> { ["bootstrap.servers"] = "localhost:9092" };
await using var producer = await AsyncKafkaProducer<string, string>.Create(
    config, new AsyncStringSerializer(), new AsyncStringSerializer());
AsyncKafkaFuture<RecordMetadata> delivery =
    await producer.Send(new ProducerRecord<string, string>("orders", "key", "value"));
RecordMetadata metadata = await delivery.Get();
```

A serializer that waits, as one that reads a schema registry does, implements the asynchronous
interface directly. A synchronous serializer of your own is given to an asynchronous client
through a class you write that calls it and returns a completed `ValueTask`; the package has no
adapter for it:

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Common.Serialization;

public sealed class AsyncOrderSerializer(ISerializer<Order> inner) : IAsyncSerializer<Order>
{
    public ValueTask<ReadOnlyMemory<byte>?> Serialize(
        string topic, Order? data, CancellationToken cancellationToken = default) =>
        new ValueTask<ReadOnlyMemory<byte>?>(inner.Serialize(topic, data));
}
```

An asynchronous producer awaits such a serializer for the key and then for the value, and the
record enters the producer when both have completed, so records enter in the order their
serialization completes. A caller that needs them in `Send` order awaits the first stage of one
`Send` (`await producer.Send(record)`) before it calls the next. A serializer that fails, by
throwing or through its `ValueTask`, fails that first stage and nothing is sent, except with an
`ApiException` on an `AsyncKafkaProducer`: that fails the record through the future `Send` hands
back, as a built-in serializer's does and as Java's `KafkaProducer.doSend`. A record whose token
is cancelled, or whose producer is closed, while a serializer is awaited is not sent, although on
an `AsyncKafkaProducer` its headers are then already read-only.

An `AsyncKafkaProducer` or `AsyncKafkaConsumer` disposes its serializers or deserializers, given
or built from its configuration, only through `IAsyncDisposable`: it never calls
`IDisposable.Dispose()` on one, and its `Create` refuses one that is `IDisposable` without being
`IAsyncDisposable`. The
asynchronous mocks dispose none. Prefer `await using` or `DisposeAsync()` to `Dispose()`, which
blocks its thread until the client is closed and its serdes are disposed. A serde's
`DisposeAsync()` is always called with no synchronization context and with the default task
scheduler current, so an `await` in it never needs the thread that waits.

### Reading a key or value without copying it

A deserializer is normally given a `ReadOnlySpan<byte>` that is valid only during its call. On a
synchronous consumer, `ReadOnlyMemoryDeserializer`, or a deserializer of your own that implements
`IDeserializerWithReadOnlyMemory<T>`, is given a `ReadOnlyMemory<byte>` over the consumer's own
bytes instead, which it may keep or return:

```csharp
using System;
using System.Buffers;
using System.Collections.Generic;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

var config = new Dictionary<string, object?>
{
    ["bootstrap.servers"] = "localhost:9092",
    ["group.id"] = "orders-reader",
    ["group.protocol"] = "consumer",
};
using var consumer = KafkaConsumer<string, ReadOnlyMemory<byte>?>.Create(
    config, new StringDeserializer(), new ReadOnlyMemoryDeserializer());
consumer.Subscribe(new[] { "orders" });
var kept = new List<ReadOnlyMemory<byte>>();
foreach (var record in consumer.Poll(TimeSpan.FromSeconds(1)))
{
    if (record.Value() is ReadOnlyMemory<byte> value)
    {
        kept.Add(value);
        using MemoryHandle pin = value.Pin();
        Console.WriteLine($"{record.Key()}: {value.Span.Length} bytes");
    }
}
```

On an asynchronous consumer, the memory an `IAsyncDeserializer<T>` is given is valid until the
`ValueTask` it returns completes, so across its awaits; reading it after that throws
`ObjectDisposedException`. A deserializer that implements `IAsyncDeserializerWithRetention<T>`, an
interface with no member of its own that extends `IAsyncDeserializerWithHeaders<T>`, may keep or
return the memory its headers form is given (the form the consumer calls on it), as
`AsyncReadOnlyMemoryDeserializer` does.

What to know about a memory that was kept:

- It stays valid while it, or a slice of it, is reachable: past the next `Poll` and past the
  consumer's disposal. It keeps every record polled with it in memory until then. The garbage
  collector releases them, and is told how many bytes they hold; nothing is disposed, and
  `ConsumerRecords` is not `IDisposable`.
- A span read from it does not keep the bytes valid by itself. Hold
  `using MemoryHandle pin = memory.Pin();` while reading a span that may outlive the memory.
- `ReadOnlyMemory<byte>` has no position or limit, as Java's `ByteBuffer` has: it is exactly the key
  or the value. It compares by identity, so compare the bytes with `a.Span.SequenceEqual(b.Span)`.
- Header values are copied, as before.

A record consumed this way is produced again without a copy by giving the memory to a
`ReadOnlyMemorySerializer`: the producer reads the consumer's bytes where they are.

```csharp
using var consumer = KafkaConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>.Create(
    consumerConfig, new ReadOnlyMemoryDeserializer(), new ReadOnlyMemoryDeserializer());
using var producer = KafkaProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>.Create(
    producerConfig, new ReadOnlyMemorySerializer(), new ReadOnlyMemorySerializer());
consumer.Subscribe(new[] { "orders" });
foreach (var record in consumer.Poll(TimeSpan.FromSeconds(1)))
{
    producer.Send(new ProducerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
        "orders-copy", record.Key(), record.Value()));
}
```

### Breaking changes since 4.4.0-alpha.1

- `ByteBufferSerializer`, `ByteBufferDeserializer`, `Serdes.ByteBufferSerde` and `Serdes.ByteBuffer()`
  are now `ReadOnlyMemorySerializer`, `ReadOnlyMemoryDeserializer`, `Serdes.ReadOnlyMemorySerde` and
  `Serdes.ReadOnlyMemory()`, after the .NET type they handle. The old names are gone.
- `KafkaProducer`, `KafkaConsumer`, `AsyncKafkaProducer` and `AsyncKafkaConsumer` have no public
  constructor: write `KafkaProducer<K, V>.Create(config, key, value)` where you wrote
  `new KafkaProducer<K, V>(config, key, value)`, and `await AsyncKafkaProducer<K, V>.Create(...)` for
  an asynchronous client. The mocks keep their constructors.
- The asynchronous clients take `IAsyncSerializer<T>` and `IAsyncDeserializer<T>` where they took
  `ISerializer<T>` and `IDeserializer<T>`: pass the `Async` class of a built-in, or an asynchronous
  class of your own. Its plain method is `Serialize(topic, data, cancellationToken)` /
  `Deserialize(topic, data, cancellationToken)`; one that reads the headers also implements
  `IAsyncSerializerWithHeaders<T>` / `IAsyncDeserializerWithHeaders<T>`.

## Examples

Runnable producer, consumer and admin examples, blocking and async, are in
[dotnet/examples](https://github.com/confluentinc/kafka-clients/tree/master/dotnet/examples).

The verifiable producer and consumer, which run Kafka's system tests against this client, are in
[dotnet/tools/VerifiableClients](https://github.com/confluentinc/kafka-clients/tree/master/dotnet/tools/VerifiableClients).

## License

Apache License 2.0. The source is at https://github.com/confluentinc/kafka-clients.
