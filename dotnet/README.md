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
using var producer = new KafkaProducer<string, string>(config, Serdes.String, Serdes.String);
var record = new ProducerRecord<string, string>("orders", value: "value", key: "key");
RecordMetadata metadata = producer.Send(record).Get();
```

The value comes before the key in the `ProducerRecord` constructor, unlike Java's
`ProducerRecord(topic, key, value)`, so name the arguments.

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
using var consumer = new KafkaConsumer<string, string>(config, Serdes.String, Serdes.String);
consumer.Subscribe(new[] { "orders" });
foreach (var record in consumer.Poll(TimeSpan.FromSeconds(1)))
{
    Console.WriteLine($"{record.Key}: {record.Value}");
}
```

The consumer supports the consumer group protocol of KIP-848 only, so `group.protocol` must be
`consumer`.

## License

Apache License 2.0. The source is at https://github.com/confluentinc/kafka-clients.
