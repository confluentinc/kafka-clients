# Changelog

All notable changes to the `Confluent.Kafka` .NET package built on the Rust core
are documented in this file.

Before GA the public API is not stable, and an interface may change even in a
minor release.

## [4.4.0-beta.0.dev.1] - 2026-11-27

### Security

- Without `ssl.truststore.location` or `ssl.truststore.certificates`, the
  broker certificate is verified by the operating system's certificate
  verifier (its root certificates on Linux, where `SSL_CERT_FILE` /
  `SSL_CERT_DIR` override them), as the Java client uses the JDK's default
  trust store (the core's `platform-verifier` feature); the package's native
  libraries no longer ship Mozilla's root bundle. On Linux a host without a CA
  bundle must set one of the truststore keys or `SSL_CERT_FILE`. On Windows
  the verifier also checks online whether the certificate was revoked: on a
  network without internet access the handshake can wait up to 10 seconds.

### Platforms

- **Breaking:** the package no longer has a `net8.0` build: it targets
  `net462`, `netstandard2.0` and `net10.0`. A .NET 8 or 9 application gets the
  `netstandard2.0` build, which runs there, Native AOT included, but is no
  longer a supported setup.
- Native AOT and trimming: the `net10.0` build is AOT-compatible and
  trimmable, and is checked by the trim and AOT analyzers. A map in an error
  message that is only a generic `IDictionary<,>` or `IReadOnlyDictionary<,>`
  now prints as the list of its pairs (`[[a, 1]]`), while a string-to-object
  map keeps Java's `{a=1}`. An `IList` given for a `LIST` config is taken as a
  list, even when it is also a generic map.
- The `net462` and `netstandard2.0` builds depend on
  `Microsoft.Bcl.AsyncInterfaces` 10.0.15 (was 8.0.0, out of support) and
  `System.Memory` 4.6.3 (was 4.5.5), so a `net462` application needs no
  binding redirect for them.
- The `netstandard2.0` build refuses a `Half`, `Int128` or `UInt128` value for
  a `DOUBLE` config key, since it has none of these types.

### Producer

- `MockProducer` and `AsyncMockProducer` take their injected errors through
  setters named after Java's public fields (`SendException`, `FlushException`,
  `CloseException`, ...), without the `Set` prefix.
- **Breaking:** the mock producers' optional `autoComplete` parameter is gone:
  they have a two-argument constructor, and a three-argument one that takes
  `autoComplete` first, in Java's order (`new MockProducer<K, V>(autoComplete,
  keySerializer, valueSerializer)`).
- A null send callback (`ICallback` or `IAsyncCallback`) sends the record
  without a callback, as Java's `send(record, null)` does, on all four
  producers; it used to throw `ArgumentNullException`. On `IAsyncProducer` a
  bare `Send(record, null)` is ambiguous: cast the null.
- `Send` reports a closed producer before a null record, as Java does: it
  throws `IllegalStateException` ("Cannot perform operation after producer
  has been closed", or the mock's "MockProducer is already closed.") where it
  threw `ArgumentNullException`. The mocks also report a fenced producer or an
  injected send exception before a null record.
- A header with a null key throws Java's `IllegalArgumentException` ("Invalid
  null header key found in headers") on all four producers.
- `Flush` called from a delivery callback of `KafkaProducer` or
  `AsyncKafkaProducer` logs Java's "may lead to deadlock" message at ERROR
  (`Trace.TraceError`) before it throws, as Java does.
- `PartitionsFor` on `KafkaProducer` and `AsyncKafkaProducer` refuses a topic
  holding a NUL character with `ArgumentException` ("A topic must not contain
  a NUL character: ..."). Before, the core read the topic cut at the NUL and
  answered for another topic. The mocks return an empty list, as Java's mock
  does.
- `SendOffsetsToTransaction` refuses a NUL character in a topic or in offset
  metadata with `ArgumentException` ("Offset metadata must not contain a NUL
  character: ..." for the metadata).
- With the default buffer allocation strategy, the core's producer copies a
  record once on its way to the socket instead of twice, and no longer
  zero-fills the buffers it allocates, so it uses less memory and CPU per
  byte sent.

### Consumer

- **Breaking:** `MockConsumer` and `AsyncMockConsumer` have Java's shape. They
  have one constructor, taking the offset reset strategy (`new
  MockConsumer<K, V>("latest")`), which is required: a null throws Java's
  `IllegalArgumentException` ("Auto offset reset strategy is null"). They take
  no deserializers, and `AddRecord(ConsumerRecord<K, V>)` adds a typed record
  whose key and value objects `Poll` returns.
- **Breaking:** the mock consumers' helpers take Java's arguments:
  `UpdateBeginningOffsets` and `UpdateEndOffsets` take an
  `IReadOnlyDictionary<TopicPartition, long>` (one call can set several
  partitions; a negative partition is stored, as in Java), and
  `UpdatePartitions(topic, partitions)` takes the `PartitionInfo` list
  `PartitionsFor` answers. They replace `UpdateBeginningOffset`,
  `UpdateEndOffset(topic, partition, offset)` and the five-argument
  `UpdatePartitions`.
- **Breaking:** `CommitAsync(offsets)` is gone, as Java has no
  `commitAsync(Map)`: write `CommitAsync(offsets, null)`, or `CommitAsync(
  offsets, (IOffsetCommitCallback?)null)` on `IAsyncConsumer`.
  `EnforceRebalance()` and `EnforceRebalance(reason)` are two overloads where
  there was an optional parameter. Recompile code built against the optional
  parameters.
- **Breaking:** `ConsumerHandle` and `IConsumerCommon.Handle()` are no longer
  public. Inside a callback, call the consumer itself.
- A `KafkaException` a rebalance listener throws (`WakeupException`,
  `CommitFailedException`, ...) is thrown as the same instance, as Java does,
  by `Poll`, `Unsubscribe`, an empty `Subscribe` or `Assign` and every other
  waiting call. It used to be wrapped in `KafkaException("User rebalance
  callback throws an error")`, so a `catch (WakeupException)` around `Poll`
  did not match. Any other exception keeps that wrapper, with the listener's
  exception as `InnerException`, and `Close` still wraps in
  `KafkaException("Failed to close kafka consumer")`.
- An asynchronous consumer call whose error the binding fails to convert now
  faults its `Task` with the converter's exception. Before, the `Task` never
  completed.
- Every consumer method that takes a topic, on the four consumers, refuses
  one that holds a NUL character with `ArgumentException` ("A topic must not
  contain a NUL character: ..."), and `Commit` and `CommitAsync` refuse a NUL
  in offset metadata. Before, the core read such a topic cut at the NUL.
  Where Java looks the partition up in the assignment (`Seek`, `Pause`,
  `Resume`, `SeekToBeginning`, `SeekToEnd`, `Position`, `CurrentLag`), a NUL
  topic gets Java's answer for a partition that is not assigned.
- A real consumer's `BeginningOffsets` or `EndOffsets` refuses a null
  partition with `ArgumentNullException` ("partitions must not contain null")
  whatever the timeout, and the binding no longer reads
  `default.api.timeout.ms`. The mocks keep Java's mock answer.
- `AsyncMockConsumer.Rebalance` and `LosePartitions` take a
  `CancellationToken`.

### Admin client

- **Breaking:** each admin method takes Java's two overloads, `Xxx(args)` and
  `Xxx(args, XxxOptions options)`, instead of an optional `options = null`, on
  `IAdmin`, `IAsyncAdmin` and the four admin clients. A null options value now
  throws `ArgumentNullException` ("options must not be null") instead of
  using the defaults, except where Java does not read the options.
  `ListConfigResources(types)` is removed: Java's forms are `()` and `(types,
  options)`.

### Common

- **Breaking:** `KafkaProducer`, `KafkaConsumer`, `AsyncKafkaProducer` and
  `AsyncKafkaConsumer` have no public constructor: write `KafkaProducer<K,
  V>.Create(configs, key, value)`, or `await AsyncKafkaProducer<K,
  V>.Create(...)` for an asynchronous client. `Create(configs)`, or a null
  serde, builds the serde from the function the configuration holds under
  `key.serializer`, `value.serializer`, `key.deserializer` or
  `value.deserializer`, calls its `Configure` and disposes it at close. A
  serde given to `Create` is never configured.
- **Breaking:** the asynchronous clients take `IAsyncSerializer<T>` and
  `IAsyncDeserializer<T>` (with `IAsyncSerializerWithHeaders<T>`,
  `IAsyncDeserializerWithHeaders<T>` and the `WithConfigure` forms, whose
  `Configure` returns a `ValueTask`) where they took `ISerializer<T>` and
  `IDeserializer<T>`. Each built-in serde has an `Async` class
  (`AsyncStringSerializer`, ...). `AsyncKafkaProducer` and
  `AsyncKafkaConsumer` dispose their serdes only through `IAsyncDisposable`,
  and `Create` refuses one that is `IDisposable` without being
  `IAsyncDisposable`.
- A synchronous consumer can deserialize without copying: a deserializer
  implementing `IDeserializerWithReadOnlyMemory<T>`, such as
  `ReadOnlyMemoryDeserializer`, is given a `ReadOnlyMemory<byte>` over the
  polled batch, valid while it is reachable. On an asynchronous consumer the
  memory is valid until the deserializer's `ValueTask` completes, unless the
  deserializer implements `IAsyncDeserializerWithRetention<T>`.
- **Breaking:** `ByteBufferSerializer`, `ByteBufferDeserializer`,
  `Serdes.ByteBufferSerde` and `Serdes.ByteBuffer()` are renamed
  `ReadOnlyMemorySerializer`, `ReadOnlyMemoryDeserializer`,
  `Serdes.ReadOnlyMemorySerde` and `Serdes.ReadOnlyMemory()`.
- **Breaking:** the classes `Header` and `Headers` are Java's interfaces
  `IHeader` and `IHeaders`, used in every public signature. Their
  implementations are internal, as Java's `RecordHeader` and `RecordHeaders`
  are: add a header with the record's `Headers().Add(key, value)` or pass an
  `IHeader` of your own.
- **Breaking:** `TopicPartition` and `Uuid` are sealed classes, as Java's,
  instead of readonly structs: `TopicPartition?` and `Uuid?` are nullable
  references, so `.HasValue`, `.Value`, `new TopicPartition()` and
  `new Uuid()` no longer compile. `Uuid.CompareTo(null)` throws
  `ArgumentNullException`.
- **Breaking:** `KafkaFuture`, `KafkaFuture<T>`, `AsyncKafkaFuture` and
  `AsyncKafkaFuture<T>` are abstract classes, as Java's `KafkaFuture`, instead
  of readonly structs: their default is null, and a nullable future is a
  nullable reference, so `.HasValue` and `.Value` no longer compile. A null
  future given to `AllOf`, or reaching a result's `All()`, throws
  `ArgumentNullException` ("futures must not contain null").
- **Breaking:** `ConsumerGroupMetadata` is the interface
  `IConsumerGroupMetadata`, as Java's class becomes one in Kafka 5.0
  (KIP-1136): `GroupMetadata()` returns it and `SendOffsetsToTransaction`
  takes it, including an implementation of your own, whose null `GroupId()`
  or `MemberId()` throws `ArgumentNullException` ("group.id can't be null",
  "member.id can't be null").
- **Breaking:** no class has an `==` or `!=` operator, so `==` compares
  references, as in Java: `TopicPartition`, `Uuid` and `MetricName` had
  operators that compared values (use `Equals`). The futures also lose `==`,
  `!=`, `IEquatable<...>`, `Equals` and `GetHashCode`, which compared
  references, so their answers do not change. Rebuild an application that
  used these operators.
- **Breaking:** a parameter Java types as a collection takes its read-only
  counterpart: `IReadOnlyCollection<T>` for a `Collection` or a `Set` and
  `IReadOnlyList<T>` for a `List` (`CreateTopics`, `CreateAcls`, `DeleteAcls`,
  `AlterClientQuotas`, `AlterUserScramCredentials`,
  `DescribeUserScramCredentials`, `TopicCollection.OfTopicNames` /
  `OfTopicIds` and the constructors of the admin value classes). A list, an
  array or a set works as before; a LINQ query needs `ToList()`.
- A null that Java would dereference (a null element, key or value, or a
  `TopicPartition` with a null topic) throws `ArgumentNullException` with the
  parameter's name: "<param> must not contain null" or "<param> must not
  contain a TopicPartition with a null topic". This applies to the consumer's
  commits and offset lookups, the producer's `SendOffsetsToTransaction` and
  the admin client. It used to be a plain `ArgumentException` with the
  binding's own text.
