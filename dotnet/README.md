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

- .NET 8 and .NET 10, and .NET Framework 4.6.2 or later on Windows; the .NET Standard 2.0
  assembly serves other .NET runtimes on these platforms
- Linux x64 and arm64, with glibc 2.28 or later (RHEL 8, Debian 10, Ubuntu 20.04 and later)
- macOS arm64 (Apple silicon): the native library needs macOS 11 or later, and each .NET version
  has its own macOS minimum ([.NET 8](https://github.com/dotnet/core/blob/main/release-notes/8.0/supported-os.md),
  [.NET 10](https://github.com/dotnet/core/blob/main/release-notes/10.0/supported-os.md))
- Windows x64 and x86, Windows 10 or Windows Server 2016 or later. The native library links the
  C runtime statically, so it needs no Visual C++ Redistributable.

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
using var producer = new KafkaProducer<string, string>(config, new StringSerializer(), new StringSerializer());
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
using var consumer = new KafkaConsumer<string, string>(config, new StringDeserializer(), new StringDeserializer());
consumer.Subscribe(new[] { "orders" });
foreach (var record in consumer.Poll(TimeSpan.FromSeconds(1)))
{
    Console.WriteLine($"{record.Key()}: {record.Value()}");
}
```

The consumer supports the consumer group protocol of KIP-848 only, so `group.protocol` must be
`consumer`.

## Serializers and deserializers

A client takes one serializer or deserializer for keys and one for values, as constructor arguments.
`Confluent.Kafka.Common.Serialization` has built-in ones for strings, byte arrays,
`ReadOnlyMemory<byte>?`, booleans, the integer and floating-point types and `Guid`, named as in Java
(`StringSerializer`, `LongDeserializer`, `UUIDSerializer`).

### Synchronous and asynchronous clients

The synchronous clients (`KafkaProducer`, `KafkaConsumer` and their mocks) take `ISerializer<T>` and
`IDeserializer<T>`. The asynchronous clients (`AsyncKafkaProducer`, `AsyncKafkaConsumer` and their
mocks) take only `IAsyncSerializer<T>` and `IAsyncDeserializer<T>`, whose one method returns a
`ValueTask`, always receives the headers, and receives the `CancellationToken` of the `Send` or
`Poll` it serves. Each built-in has an asynchronous class, `Async` and its name
(`AsyncStringSerializer`, `AsyncLongDeserializer`); a client given two of them serializes or
deserializes inside the call and awaits nothing.

```csharp
using System.Collections.Generic;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;

var config = new Dictionary<string, object?> { ["bootstrap.servers"] = "localhost:9092" };
await using var producer = new AsyncKafkaProducer<string, string>(
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
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;

public sealed class AsyncOrderSerializer(ISerializer<Order> inner) : IAsyncSerializer<Order>
{
    public ValueTask<ReadOnlyMemory<byte>?> Serialize(
        string topic, Headers headers, Order? data, CancellationToken cancellationToken) =>
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
is cancelled, or whose producer is closed, while a serializer is awaited is not sent, although
its headers are then already read-only.

An asynchronous client disposes the serializers or deserializers it was given, through
`IAsyncDisposable` when they have it and otherwise through `IDisposable`. Prefer `await using` or
`DisposeAsync()` to `Dispose()`, which blocks its thread until the client is closed and its serdes
are disposed. A serde's `DisposeAsync()` is always called with no synchronization context and with
the default task scheduler current, so an `await` in it never needs the thread that waits.

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
using var consumer = new KafkaConsumer<string, ReadOnlyMemory<byte>?>(
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
interface with no member of its own, may keep or return the memory, as
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
using var consumer = new KafkaConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
    consumerConfig, new ReadOnlyMemoryDeserializer(), new ReadOnlyMemoryDeserializer());
using var producer = new KafkaProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
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
- The constructors of the asynchronous clients take `IAsyncSerializer<T>` and
  `IAsyncDeserializer<T>` where they took `ISerializer<T>` and `IDeserializer<T>`: pass the `Async`
  class of a built-in, or an asynchronous class of your own.

## Examples

Runnable producer, consumer and admin examples, blocking and async, are in
[dotnet/examples](https://github.com/confluentinc/kafka-clients/tree/master/dotnet/examples).

## License

Apache License 2.0. The source is at https://github.com/confluentinc/kafka-clients.
