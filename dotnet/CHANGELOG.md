# Changelog

All notable changes to the `Confluent.Kafka` .NET package are documented in this file.

Before GA the public API is not stable, and an interface may change even in a minor release.

## [4.4.0-beta.1] - Unreleased

First preview release of the 4.4.0 .NET client, built on the Confluent Kafka
Rust client, which the package ships as a native library. Its API is the Apache
Kafka Java client's (4.4.0), in C# form.

> **Preview:** not yet recommended for production use. The public API is not
> stable before the GA release and may change. We welcome your feedback while
> the design is still open.

### Clients

- Producer, consumer and admin client, each as a blocking class
  (`KafkaProducer`, `KafkaConsumer`, `KafkaAdminClient`) and an async peer
  (`AsyncKafkaProducer`, `AsyncKafkaConsumer`, `AsyncKafkaAdminClient`).
- Mock clients for tests (`MockProducer`, `MockConsumer`, `MockAdminClient`),
  each with an async peer.
- The producers and consumers are built by their static `Create`, the admin
  clients by `Admin.Create` / `AsyncAdmin.Create`.

### Producer

- Send with a callback and a future for the record's metadata; flush and close.
  The async `Send` is awaited twice, once for the hand-over and once for the
  delivery: `await (await producer.Send(record)).Get()`.
- Idempotent producer and transactions.

### Consumer

- KIP-848 consumer group protocol: subscribe to topics or to a broker-side
  pattern, or assign partitions manually.
- Rebalance listeners, which can commit, seek and query the consumer from
  inside the callback.
- Offset commits (waiting and not waiting), seek and offset lookups, pause and
  resume, and `Wakeup()`.
- Zero-copy reads: `ReadOnlyMemoryDeserializer` hands out the consumer's own
  bytes, which a `ReadOnlyMemorySerializer` produces again without a copy.

### Admin client

- Topics and partitions, configurations, consumer groups and offsets, ACLs,
  quotas, SCRAM credentials, delegation tokens, transactions, features and log
  directories. Each operation returns its result at once, with one future per
  key, as in Java.

### Common

- Java's API in C# form: Java's names in PascalCase, getters as methods, Java's
  overloads, and each type in the namespace of its Java package
  (`Confluent.Kafka.Producer`, `Confluent.Kafka.Common.Serialization`).
- Typed errors, one class per Java exception, caught by class
  (`catch (RetriableException)`).
- One future type, `KafkaFuture` / `AsyncKafkaFuture`, for every future the
  clients hand out.
- Built-in serializers and deserializers (strings, byte arrays,
  `ReadOnlyMemory<byte>?`, booleans, integers, floating point, `Guid`), with
  async forms for the async clients; a serializer can also come from the
  configuration.
- Configuration with the Java client's keys, as a dictionary of typed values.
- Without a truststore, the broker certificate is verified by the operating
  system's certificate verifier. On Linux, a host without a CA bundle must set
  a truststore key or `SSL_CERT_FILE`.
- Builds for .NET 10, .NET Framework 4.6.2 and .NET Standard 2.0, with Native
  AOT on .NET 10. Native libraries for Linux x64 and arm64 (glibc 2.28 or
  later), macOS arm64, and Windows x64 and x86.

### Known limitations

- Not compatible with the API of the librdkafka-based Confluent.Kafka 2.x. The
  two share the package id, so a project gets one of them for its whole
  dependency graph, and packages built on 2.x, such as
  Confluent.SchemaRegistry.Serdes, do not work with this version.
- Only the KIP-848 consumer group protocol: set `group.protocol=consumer`.
  `classic`, Java's default, is rejected.
- The share consumer (KIP-932) is not included.
- Limited pluggable interfaces: no custom partitioners, interceptors or metric
  reporters.
- Of the SASL mechanisms, only `PLAIN` is supported. Keys and certificates must
  be PEM (no JKS or PKCS12).
- KIP-714 client telemetry (`clientInstanceId` and the metric subscriptions)
  is not offered, and the producer's metrics lack Java's network metrics.
- macOS x64 (Intel), Windows arm64 and musl-based Linux (Alpine) are not
  supported yet. .NET 8 and 9 run on the .NET Standard 2.0 build, but are not
  supported.
