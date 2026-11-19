# RFC: Python client API

- **Status:** Draft
- **Audience:** users of Confluent's Python client

## Summary

The Python client is a thin binding over the C API of the [Rust client](rust.md). It is the next major version of the `confluent-kafka` package, which is built on librdkafka today.

Like the Rust client, its API is the Apache Kafka Java client's API. Java decides the content: the classes, methods, parameters, types, defaults and behaviour. Python decides only the form: names, keyword arguments, exceptions, context managers and `asyncio`. Best practice and guidance for using the Apache Kafka Java client also apply to the Python client.

Where Python cannot express what Java does, or following Java would make the client much harder to use, the Python client deviates from Java. Each deviation is written down, with its reason, in the rules described under [API shape](#api-shape).

## Distribution

* **Package:** published to PyPI as `confluent-kafka`
* **Install:** `pip install confluent-kafka`
* **Import:** `confluent_kafka`, one module per Java package, e.g. `from confluent_kafka.producer import KafkaProducer`
* **Python:** CPython 3.10 to 3.15
* **Wheels:** prebuilt for Linux (manylinux 2.28, x86_64 and aarch64) and macOS (x86_64 and arm64), with the Rust core inside. There is no Windows wheel.

## API shape

The client follows the rules in the `Python Binding Conventions` section of this repository's `CLAUDE.md`, next to the Rust translation rules. Its surface is generated from those rules, the Java source and the Rust core's C header, so the same inputs always give the same API.

At the API level the key points are:

* **Java's API with keyword-only parameters.** Every parameter is keyword-only and keeps its Java name. One Python method covers a whole Java overload set.

* **A sync client and an asyncio client.** Each client class has an `asyncio` peer with the same methods, named `Async` + the class name, in the same module.

* **Typed exceptions.** Each Java exception class is a Python exception class with Java's parent chain, so you catch errors by class.

* **One future type.** Every future the package hands out is a `KafkaFuture` (sync) or an `AsyncKafkaFuture` (asyncio), with Java's `KafkaFuture` methods.

* **`None` is Java's `null`.** Leaving a parameter out selects the Java overload without it. Passing `None` passes Java's `null`.

* **No copy of keys and values.** A serializer's `bytes` reach the core without a copy, and the core writes them once into the record batch, as Java does. A deserializer gets a `memoryview` into the fetch buffer. Header values are copied once, on both paths.

* **Fully typed.** The package ships annotations and `.pyi` stubs, with one stub per Java overload. `mypy --strict` infers the key and value types from the serializers.

## Relationship to the Rust core

The Python client does no Kafka protocol work of its own. Every call on `KafkaProducer`, `KafkaConsumer` and `KafkaAdminClient` goes through the Rust core's C API. So the Python client behaves as the core does, and a fix in the core reaches Python without a change in Python.

The value types, the `*Options` and `*Result` classes, the errors, the serializers and the mock clients are plain Python, translated from their Java source.

## The Python client interface

### Naming conventions

The names in Java have a clear mapping to their equivalents in Python. The following table gives some examples:

| Java | Python |
|---|---|
| `package org.apache.kafka.clients.consumer` | `module confluent_kafka.consumer` |
| `package org.apache.kafka.common.header` | `module confluent_kafka.common.header` |
| `class ProducerRecord` (PascalCase) | `class ProducerRecord` (PascalCase) |
| `method offsetsForTimes(timestampsToSearch)` (camelCase) | `offsets_for_times(*, timestamps_to_search)` (snake_case) |
| getter `Uuid.getMostSignificantBits()` | `most_significant_bits()` |
| getter `TopicPartition.topic()` | `topic()`, a method, not a property |
| `const ConsumerRecord.NO_TIMESTAMP` | class attribute `ConsumerRecord.NO_TIMESTAMP` |
| `class TopicAuthorizationException` | `class TopicAuthorizationError` |
| `commitSync()` / `commitAsync()` | `commit()` / `commit_nowait()` |
| `null`, `Optional.empty()` | `None` |
| `java.time.Duration` | seconds as a `float`, or a `datetime.timedelta` |
| `AutoCloseable` | a context manager (`with`, `async with`) |
| `toString()`, `equals()`, `hashCode()` | `__str__`, `__eq__`, `__hash__` |
| `iterator()`, `count()` | `__iter__`, `__len__` |

- Each type lives in the module of its Java package, with `clients` dropped. Import a name from its module: the package root exports only `Duration` and the Java built-in exceptions (`IllegalArgumentError`, `IllegalStateError`, `TimeoutError`, `ExecutionError`, …).
- Java's `xSync` / `xAsync` pairs become `x()` / `x_nowait()`. On an asyncio class "async" means a coroutine, so Java's `commitAsync` name would mark the one method that does not wait for the broker. `_nowait` is Python's suffix for that, as in `queue.Queue.put_nowait`.

### Sync and asyncio clients

Each client class has an asyncio peer in the same module:

| Java | Sync | Asyncio |
|---|---|---|
| `Producer`, `KafkaProducer`, `MockProducer` | same names | `AsyncProducer`, `AsyncKafkaProducer`, `AsyncMockProducer` |
| `Consumer`, `KafkaConsumer`, `MockConsumer` | same names | `AsyncConsumer`, `AsyncKafkaConsumer`, `AsyncMockConsumer` |
| `Admin`, `KafkaAdminClient`, `MockAdminClient` | same names | `AsyncAdmin`, `AsyncKafkaAdminClient`, `AsyncMockAdminClient` |

- On an asyncio class, a method is `async def` when it waits for the Rust core. This covers every method that blocks in Java, and the consumer methods that hand work to the core's background task (for example `assign`, `seek` and `pause`). Reads of local state (for example `assignment()`, `paused()` and `metrics()`) and `wakeup()` stay plain `def`.
- On a sync class the same methods wait in short steps, so a Ctrl+C is seen during any wait and raises `KeyboardInterrupt`. An interrupted `poll()` seeks back, so no record is skipped.
- The admin operations are plain `def` on both classes. They return their result at once, as Java's do, and you wait on the futures it holds.
- An asyncio client is bound to one event loop: the loop running when it is built, or else the loop of its first async call.

### Callbacks and threads

- The `send()` callback runs on a background completion thread for `KafkaProducer`, and on the event loop for `AsyncKafkaProducer`. Callbacks run one at a time, in completion order, before the record's future completes. There is no producer `poll()`.
- The rebalance listener and the `commit_nowait()` callback run on the caller's thread, inside a call to the consumer. As in Java, the listener runs inside `poll()`, `unsubscribe()` or `close()`, and the commit callback inside a later `poll()`, `commit()`, `commit_nowait()` or `close()`. The rebalance does not move on until the listener returns.
- A listener may call back into its consumer directly, as in Java: `commit()`, `seek()`, `position()`, `pause()`, and so on. Inside a listener, `assign`, `subscribe`, `subscription`, `unsubscribe`, `poll` and `close` raise `ConcurrentModificationError`.
- On the asyncio classes, a callback or a listener method may be `async def`, and the client awaits it on the event loop.
- `KafkaProducer` and `KafkaAdminClient` are thread-safe. `KafkaConsumer` is not: a call while another thread is inside it raises `ConcurrentModificationError`. `wakeup()` and `metrics()` work from any thread, as in Java.

### Overloaded methods

Python has no method overloading, but it has keyword arguments. So one Java overload set becomes one Python method, which takes the union of the overloads' parameters, each keyword-only and under its Java name. For example, Java's `KafkaConsumer.commitSync` has four overloads:

```java
void commitSync();
void commitSync(Duration timeout);
void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);
void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout);
```

The Python equivalent is one method, with one `@typing.overload` stub per Java overload:

```python
@overload
def commit(self) -> None: ...
@overload
def commit(self, *, timeout: Duration) -> None: ...
@overload
def commit(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata]) -> None: ...
@overload
def commit(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata], timeout: Duration) -> None: ...
```

- The parameters you pass select the Java overload. `None` counts as given: see [`None` and leaving a parameter out](#none-and-leaving-a-parameter-out).
- A set of parameters that matches no Java overload raises `IllegalArgumentError`, which lists the forms Java has: `commit_nowait(offsets=o)` raises `commit_nowait() takes one of (), (callback), (offsets, callback); got (offsets)`.
- A positional call raises `TypeError`. Because every call names its arguments, it keeps working when Java adds a parameter or makes one optional.
- Java's deprecated methods and overloads are not translated. For example, there is no consumer `close(Duration)`: pass `close(option=CloseOptions(timeout=…))` instead.

### `None` and leaving a parameter out

Java has `null`, and an overload without the parameter. Python has `None`, and a parameter you leave out. The client keeps the two apart, as Java does:

| Java | Python | Result |
|---|---|---|
| `consumer.close()` | `consumer.close()` | closes with the default options |
| `consumer.close(null)` | `consumer.close(option=None)` | Java throws `NullPointerException`; Python raises `TypeError("option must not be None")` |
| `new ProducerRecord<>("t", null, null, v)` | `ProducerRecord(topic="t", partition=None, key=None, value=v)` | the producer picks the partition |

- Leaving a parameter out selects the Java overload without it. It is the only way to say "not given".
- `None` is Java's `null`, so it counts as given and selects the overload that takes the parameter.
- Where Java rejects `null`, `None` raises the same error, with `TypeError` in place of `NullPointerException`. Where Java gives `null` a meaning, `None` keeps it.
- A parameter's type shows whether `None` is allowed: `partition: int | None`, but `option: CloseOptions`. So `mypy` flags a `None` that Java would reject.

### Getters and setters

- A Java getter stays a method: `record.topic()`, `metadata.offset()`. There are no properties, and no public attributes apart from constants and nested types.
- A getter and a setter that share one Java name are one Python method: with no argument it reads, with one it sets. For example, `AbstractOptions.timeoutMs()` / `timeoutMs(Integer)` become `options.timeout_ms()` / `options.timeout_ms(timeout_ms=5000)`.
- A class Java builds with setters (the `*Options` classes, `CloseOptions`) also takes each setter's field as a constructor keyword, so it can be built in one call: `CreateTopicsOptions(timeout_ms=5000, validate_only=True)`. Java's setter chain still works: `CreateTopicsOptions().validate_only(validate_only=True)`.

### Error handling

Python can express Java's exception hierarchy, so every Java exception class has a Python class with Java's name, `Exception` replaced by `Error`, and Java's parent chain. Java's `KafkaException` is `KafkaError`, a subclass of `RuntimeError`. You catch an error by its class:

```python
from confluent_kafka.common.errors import RetriableError

try:
    consumer.commit()
except RetriableError:
    ...  # retry
```

- There is no `code()` and no `is_retriable()`: the class is the test. A class Java adds later is a subclass of its Java parent, so existing `except` clauses keep catching it.
- Each error class lives in the module of its Java package: `TopicExistsError` in `confluent_kafka.common.errors`, `CommitFailedError` in `confluent_kafka.consumer`.
- The Java built-in exceptions are at the package root. Some also extend a Python built-in, so plain Python code catches them too: `IllegalArgumentError` is a `ValueError`, `TimeoutError` subclasses Python's own `TimeoutError`, and `UnsupportedOperationError` is a `NotImplementedError`. As in Java, they are not `KafkaError`s.
- `str(e)` is Java's `getMessage()`, and `e.__cause__` is Java's `getCause()`.
- An exception that your own code raises in a serializer, a callback or a listener comes back out of the client as the same object.

### Futures

Every future the package hands out is a `KafkaFuture[T]` on a sync client, or an `AsyncKafkaFuture[T]` on an asyncio client, both in `confluent_kafka.common`. They have Java's `KafkaFuture` methods: `get`, `get_now`, `then_apply`, `when_complete`, `all_of`, `cancel`, `is_done`, and so on. `get` is `async def` on `AsyncKafkaFuture`.

- `get()` on a failed future raises `confluent_kafka.ExecutionError`, as Java's `get()` throws `ExecutionException`. The error itself is `e.__cause__`.
- `get(timeout=…)` raises `TimeoutError` when the time runs out.
- The future that `send()` returns is a `KafkaFuture` too, so it also has `then_apply` and `when_complete`, which Java's `Future<RecordMetadata>` lacks.
- A `then_apply` function or a `when_complete` action runs where the future completes: on the producer's completion thread, on the sync admin client's own thread, or on the event loop for an asyncio client.

### Serializers and deserializers

Java's `Serializer` and `Deserializer` are interfaces with one main method, so in Python a serializer or deserializer is any callable. The client calls it with keyword arguments named after Java's parameters, and the callable must accept `**kwargs`:

```python
def serialize(*, topic, data, headers=None, **kwargs) -> bytes | None: ...
def deserialize(*, topic, data, headers=None, **kwargs): ...  # data is a memoryview
```

- `confluent_kafka.common.serialization` has factories for Java's built-in serializers, matching them byte for byte: `string_serializer()`, `int_serializer(size=4)`, `float_serializer()`, `bool_serializer()`, `uuid_serializer()`, `json_serializer()`, `bytes_serializer()`, and the matching deserializers.
- You pass a serializer to the client's constructor, or name its class in the `key.serializer` / `value.serializer` config key, as in Java. With neither, the client uses the bytes serializer, where Java fails.
- `**kwargs` keeps a callable working when Java adds a parameter to the interface: the client passes it as one more keyword argument.
- A deserializer is not called for a null key or value; the record's field is `None`.
- An `async def` serializer or deserializer is rejected with `TypeError`.

The same keyword form applies to the `send()` callback (`metadata=`, `exception=`) and the `commit_nowait()` callback (`offsets=`, `exception=`). The rebalance listener is a class you subclass, and the client calls its methods with positional arguments.

### Configuration

A client takes its configuration as one `configs` dict, with the Java client's keys, values and defaults. The [Java client's configuration reference](https://kafka.apache.org/documentation/#configuration) applies, apart from the differences listed below.

```python
from confluent_kafka.producer import KafkaProducer, ProducerConfig

producer = KafkaProducer(configs={
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG: "localhost:9092",
    ProducerConfig.LINGER_MS_CONFIG: 5,
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG: True,
})
```

- The key constants are on `ProducerConfig`, `ConsumerConfig` and `AdminClientConfig`, as in Java. `TopicConfig`, `SaslConfigs`, `SslConfigs` and the other config-name classes are in `confluent_kafka.common.config`.
- A value can be a Python value (`int`, `float`, `bool`, `str`, or a `list` for a list key) or the string Java would parse. A wrong type or value raises `ConfigError` with Java's message when the client is built.
- Config keys keep Java's units. `linger.ms` is an `int` in milliseconds, but method timeouts are in seconds (`poll(timeout=0.1)`).
- As in Java, the client logs its config values at INFO when it is built, with passwords shown as `[hidden]`, then one INFO line naming the keys it did not use. A key the client does not know is accepted, and only shows up in that line.
- No config value is a callback, and no constructor takes an error callback or a logger. A failed operation raises, or fails its future.

#### Differences from Java

| Topic | Java | Python |
|---|---|---|
| Form | `Properties` or `Map<String, Object>` | one `dict[str, Any]` named `configs`. Merge a defaults chain yourself: `{**defaults, **overrides}` |
| `CommonClientConfigs` | holds the shared keys | not in the Python client: it is not public API in Kafka 4.4. Use `ProducerConfig.BOOTSTRAP_SERVERS_CONFIG` and so on |
| A `null` value | the key holds `null` | `None` means "not set", so the default applies. A key whose validator rejects `null` raises `ConfigError`, as in Java |
| `key.serializer`, `value.deserializer`, … | a Java class name | a Python class, or its dotted path. A Java class name such as `org.apache.kafka.common.serialization.StringSerializer` does not resolve: pass a built-in as a constructor argument instead (`value_serializer=string_serializer()`) |
| No serializer or deserializer | building the client fails | the bytes serializer or deserializer is used |
| Partitioner | `partitioner.class`; keys hashed with murmur2 | `partitioner.type`: `ConsistentRandomPartitioner` (the default, a CRC-32 hash as in librdkafka), `Murmur2RandomPartitioner` (Java's hash) or `RoundRobinPartitioner`. Set `Murmur2RandomPartitioner` to send each key to the same partition as a Java producer. `partitioner.class` has no effect |
| `group.protocol` | optional: `classic` (the default) or `consumer` | mandatory, and must be `consumer` (KIP-848). `classic` is not available. A consumer without `group.protocol=consumer` raises `UnsupportedOperationError` |
| SASL | `GSSAPI` (the default), `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER` | only `PLAIN`, with the user and password in `sasl.jaas.config` (`PlainLoginModule`). Any other mechanism is rejected, so set `sasl.mechanism=PLAIN` |
| SSL key and trust stores | `JKS` (the default), `PKCS12` or `PEM` | only `PEM`, which is the default. Encrypted private keys are not supported. `ssl.key.password`, `ssl.keystore.password` and `ssl.truststore.password` have no effect |
| Default trust store | the JVM's `cacerts` | Mozilla's root certificates, built into the wheel |
| JVM-only SSL keys | `ssl.provider`, `ssl.keymanager.algorithm`, `ssl.trustmanager.algorithm`, `ssl.secure.random.implementation` | checked as in Java, but have no effect |
| Keys whose value is a class | `interceptor.classes`, `metric.reporters`, `partition.assignment.strategy`, `ssl.engine.factory.class`, the `sasl.*.class` keys | not supported: accepted, but only listed as unused |
| Other keys with no effect | `config.providers`, `security.providers`, `enable.metrics.push` | not supported: accepted, but only listed as unused |
| Logging | SLF4J, set up by the application | the Python layer logs on the `confluent_kafka.*` loggers. The Rust core logs to stderr, filtered by the `RUST_LOG` environment variable, which shows errors only by default. Set `RUST_LOG=info` to see the config values and the unused keys |

### Type hints

- The package ships full annotations and `.pyi` stubs, and is marked `py.typed`.
- The clients and the records are generic in the key and value types: `KafkaConsumer[K, V]`, `ConsumerRecord[K, V]`.
- `mypy --strict` infers the types from the serializers, so you write no type parameters:

```python
consumer = KafkaConsumer(configs=conf, value_deserializer=string_deserializer())
# consumer: KafkaConsumer[bytes, str]
```

### Compared with the Rust client

Both clients follow Java, but each language expresses some things differently:

| Topic | Rust | Python |
|---|---|---|
| Overloads | one method per overload: `commit_sync_with_offsets_timeout(..)` | one method per overload set: `commit(offsets=…, timeout=…)` |
| `commitSync` / `commitAsync` | `commit_sync` / `commit_async` | `commit` / `commit_nowait` |
| Methods that block in Java | `async` | a sync class, and an asyncio peer where they are `async def` |
| Errors | one flat `Error` enum with predicates (`is_retriable_error()`) | one class per Java exception (`except RetriableError`) |
| Getter and setter with one Java name | `timeout_ms()` / `set_timeout_ms(..)` | one method, `timeout_ms()` / `timeout_ms(timeout_ms=…)` |
| Admin options | `create_topics_with_options(..)` | `create_topics(…, options=…)` |
| Calling the consumer from a listener | through a `ConsumerHandle` | through the consumer itself |

## Examples

Each example shows the same operation in the Java API and its Python translation.

### Producer example - send a record and block for its acknowledgement

#### Java

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
producer.send(new ProducerRecord<>("my-topic", "key", "value")).get();
producer.close();
```

#### Python

```python
producer = KafkaProducer(configs={"bootstrap.servers": "localhost:9092"},
                         key_serializer=string_serializer(), value_serializer=string_serializer())
producer.send(record=ProducerRecord(topic="my-topic", key="key", value="value")).get()
producer.close()
```

#### With a context manager

Java's `try`-with-resources becomes a `with` block. Leaving the block flushes the producer, then closes it, also when an exception leaves the block:

```java
try (Producer<String, String> producer =
         new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
    producer.send(new ProducerRecord<>("my-topic", "key", "value")).get();
}
```

```python
with KafkaProducer(configs={"bootstrap.servers": "localhost:9092"},
                   key_serializer=string_serializer(),
                   value_serializer=string_serializer()) as producer:
    producer.send(record=ProducerRecord(topic="my-topic", key="key", value="value")).get()
```

Every client is a context manager (`with KafkaConsumer(...)`, `with Admin.create(...)`), so the other examples can be written the same way.

With asyncio, awaiting `send()` waits for buffer space and returns the record's future, and awaiting `get()` waits for the broker. Leaving the `async with` block flushes the producer, then closes it:

```python
async with AsyncKafkaProducer(configs={"bootstrap.servers": "localhost:9092"},
                              key_serializer=string_serializer(),
                              value_serializer=string_serializer()) as producer:
    future = await producer.send(record=ProducerRecord(topic="my-topic", key="key", value="value"))
    await future.get()
```

### Consumer example - subscribe to topics and poll a batch of records in a loop

Java's four `subscribe` overloads are one Python method. The topic-collection form passes `topics=`.

#### Java

```java
KafkaConsumer<byte[], byte[]> consumer =
    new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
consumer.subscribe(Arrays.asList("foo", "bar"));
while (true) {
    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<byte[], byte[]> record : records)
        System.out.printf("offset = %d%n", record.offset());
}
```

#### Python

```python
consumer = KafkaConsumer(configs=props, key_deserializer=bytes_deserializer(),
                         value_deserializer=bytes_deserializer())
consumer.subscribe(topics=["foo", "bar"])
while True:
    records = consumer.poll(timeout=0.1)
    for record in records:
        print(f"offset = {record.offset()}")
```

### Consumer example - with a rebalance listener

Subscribe with a rebalance listener that commits the offsets before partitions are revoked, then poll a batch. In Java and in Python, this is a `ConsumerRebalanceListener` passed to `subscribe`. In Python the listener is a subclass of the abstract class `ConsumerRebalanceListener`, and the parameter keeps the name of Java's interface declaration, `callback`.

The listener runs on the thread that calls `poll()`, inside `poll()`, as in Java. So, as in Java, it can keep a reference to the consumer and call it.

#### Java

```java
consumer.subscribe(Arrays.asList("foo", "bar"), new ConsumerRebalanceListener() {
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        consumer.commitSync(); // flush offsets before the partitions move
    }
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("assigned: " + partitions);
    }
});
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, String> record : records)
        System.out.printf("offset = %d, value = %s%n", record.offset(), record.value());
}
```

#### Python

```python
class CommittingRebalanceListener(ConsumerRebalanceListener):
    def __init__(self, consumer):
        self._consumer = consumer

    def on_partitions_revoked(self, partitions):
        self._consumer.commit()  # flush offsets before the partitions move

    def on_partitions_assigned(self, partitions):
        print(f"assigned: {partitions}")

consumer.subscribe(topics=["foo", "bar"], callback=CommittingRebalanceListener(consumer))
while True:
    records = consumer.poll(timeout=0.1)
    for record in records:
        print(f"offset = {record.offset()}, value = {record.value()}")
```

### AdminClient - Create a topic and block until the request completes or fails.

Java declares each admin operation as a pair: `createTopics(newTopics)` and `createTopics(newTopics, options)`. In Python this is one method, and `options` is a keyword you may leave out.

#### Java

```java
CreateTopicsResult result = admin.createTopics(
    Set.of(new NewTopic("my-topic", 12, (short) 3)));
result.values().get("my-topic").get(); // block until created or failed
```

#### Python

```python
result = admin.create_topics(
    new_topics=[NewTopic(name="my-topic", num_partitions=12, replication_factor=3)])
result.values()["my-topic"].get()  # block until created or failed
```

When you need to override the default options, pass `options`:

```python
result = admin.create_topics(
    new_topics=[NewTopic(name="my-topic", num_partitions=12, replication_factor=3)],
    options=CreateTopicsOptions(validate_only=True))
result.all().get()  # block until created or failed
```

## Interface Details

In the listings below, each line is one Java overload, as the package's `@typing.overload` stubs list it. The `@overload` decorators are left out.

### Producer interface

The equivalent of the Java `Producer<K, V>` interface is the base class `Producer[K, V]`. It cannot be constructed. Applications use `KafkaProducer[K, V]`, and tests use `MockProducer[K, V]`, just like in Java.

```python
class Producer(Generic[K, V]):
    def init_transactions(self) -> None: ...
    def begin_transaction(self) -> None: ...
    def send_offsets_to_transaction(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
                                    group_metadata: ConsumerGroupMetadata) -> None: ...
    def commit_transaction(self) -> None: ...
    def abort_transaction(self) -> None: ...
    def send(self, *, record: ProducerRecord[K, V]) -> KafkaFuture[RecordMetadata]: ...
    def send(self, *, record: ProducerRecord[K, V],
             callback: Callback | None) -> KafkaFuture[RecordMetadata]: ...
    def flush(self) -> None: ...
    def partitions_for(self, *, topic: str) -> list[PartitionInfo]: ...
    def metrics(self) -> dict[MetricName, Metric]: ...
    def close(self) -> None: ...
    def close(self, *, timeout: Duration) -> None: ...
    # A context manager: leaving the `with` block flushes, then closes.
```

On `AsyncProducer` every method except `metrics()` is `async def`, and `send` returns an `AsyncKafkaFuture`.

- `send()` returns once the record is in the producer's buffer. It waits up to `max.block.ms` for metadata or for buffer space, as Java's does.
- `close()` waits for the records sent before it. `close(timeout=…)` waits at most `timeout`, then fails what is left.
- A `send()` that returned belongs to the `flush()`, `commit_transaction()` or `abort_transaction()` after it.
- `begin_transaction()` waits until the records sent before it are handed to the core, so it is `async def` on `AsyncProducer`. Java's does not wait.
- Java's `clientInstanceId`, `registerMetricForSubscription` and `unregisterMetricFromSubscription` are not in the Python client: the C API has no entry point for them.

The application uses `KafkaProducer[K, V]`. Java's `(Map)` and `(Properties)` constructors are both `configs`. A serializer you leave out comes from the config key, else it is the bytes serializer:

```python
class KafkaProducer(Producer[K, V]):
    def __init__(self: KafkaProducer[bytes, bytes], *, configs: dict[str, Any]) -> None: ...
    def __init__(self: KafkaProducer[K, bytes], *, configs: dict[str, Any],
                 key_serializer: Serializer[K]) -> None: ...
    def __init__(self: KafkaProducer[bytes, V], *, configs: dict[str, Any],
                 value_serializer: Serializer[V]) -> None: ...
    def __init__(self, *, configs: dict[str, Any], key_serializer: Serializer[K],
                 value_serializer: Serializer[V]) -> None: ...
```

The `ProducerRecord` looks like this. Its six Java constructors are one `__init__` (the package's stubs also bind `K` and `V` for each form; that is left out here):

```python
HeadersArg = Iterable[Header | tuple[str, bytes | bytearray | memoryview | None]]

class ProducerRecord(Generic[K, V]):
    def __init__(self, *, topic: str, partition: int | None, timestamp: int | None,
                 key: K | None, value: V | None, headers: HeadersArg) -> None: ...
    def __init__(self, *, topic: str, partition: int | None, timestamp: int | None,
                 key: K | None, value: V | None) -> None: ...
    def __init__(self, *, topic: str, partition: int | None,
                 key: K | None, value: V | None, headers: HeadersArg) -> None: ...
    def __init__(self, *, topic: str, partition: int | None,
                 key: K | None, value: V | None) -> None: ...
    def __init__(self, *, topic: str, key: K | None, value: V | None) -> None: ...
    def __init__(self, *, topic: str, value: V | None) -> None: ...
    def topic(self) -> str: ...
    def headers(self) -> Headers: ...
    def key(self) -> K | None: ...
    def value(self) -> V | None: ...
    def timestamp(self) -> int | None: ...
    def partition(self) -> int | None: ...
```

The delivery callback is Java's `Callback`. On failure `exception` is set, and every field of `metadata` but the topic and the partition is `-1`:

```python
class Callback(Protocol):
    def __call__(self, *, metadata: RecordMetadata, exception: Exception | None,
                 **kwargs: Any) -> None: ...
```

### Consumer interface

The equivalent of the Java `Consumer<K, V>` interface is the base class `Consumer[K, V]`. It cannot be constructed. Applications use `KafkaConsumer[K, V]`, and tests use `MockConsumer[K, V]`, just like in Java.

Only the KIP-848 consumer group rebalance protocol is supported, so `group.protocol` is mandatory and must be `consumer`. The `classic` protocol is not available. A consumer without `group.protocol=consumer` raises `UnsupportedOperationError`.

```python
class Consumer(Generic[K, V]):
    def assignment(self) -> set[TopicPartition]: ...
    def subscription(self) -> set[str]: ...
    def subscribe(self, *, topics: Iterable[str]) -> None: ...
    def subscribe(self, *, topics: Iterable[str], callback: ConsumerRebalanceListener) -> None: ...
    def subscribe(self, *, pattern: SubscriptionPattern, callback: ConsumerRebalanceListener) -> None: ...
    def subscribe(self, *, pattern: SubscriptionPattern) -> None: ...
    def assign(self, *, partitions: Iterable[TopicPartition]) -> None: ...
    def unsubscribe(self) -> None: ...
    def poll(self, *, timeout: Duration) -> ConsumerRecords[K, V]: ...
    def commit(self) -> None: ...
    def commit(self, *, timeout: Duration) -> None: ...
    def commit(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata]) -> None: ...
    def commit(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
               timeout: Duration) -> None: ...
    def commit_nowait(self) -> None: ...
    def commit_nowait(self, *, callback: OffsetCommitCallback | None) -> None: ...
    def commit_nowait(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
                      callback: OffsetCommitCallback | None) -> None: ...
    def seek(self, *, partition: TopicPartition, offset: int) -> None: ...
    def seek(self, *, partition: TopicPartition, offset_and_metadata: OffsetAndMetadata) -> None: ...
    def seek_to_beginning(self, *, partitions: Iterable[TopicPartition]) -> None: ...
    def seek_to_end(self, *, partitions: Iterable[TopicPartition]) -> None: ...
    def position(self, *, partition: TopicPartition) -> int: ...
    def position(self, *, partition: TopicPartition, timeout: Duration) -> int: ...
    def committed(self, *, partitions: Iterable[TopicPartition]
                  ) -> dict[TopicPartition, OffsetAndMetadata | None]: ...
    def committed(self, *, partitions: Iterable[TopicPartition], timeout: Duration
                  ) -> dict[TopicPartition, OffsetAndMetadata | None]: ...
    def metrics(self) -> dict[MetricName, Metric]: ...
    def partitions_for(self, *, topic: str) -> list[PartitionInfo]: ...
    def partitions_for(self, *, topic: str, timeout: Duration) -> list[PartitionInfo]: ...
    def list_topics(self) -> dict[str, list[PartitionInfo]]: ...
    def list_topics(self, *, timeout: Duration) -> dict[str, list[PartitionInfo]]: ...
    def paused(self) -> set[TopicPartition]: ...
    def pause(self, *, partitions: Iterable[TopicPartition]) -> None: ...
    def resume(self, *, partitions: Iterable[TopicPartition]) -> None: ...
    def offsets_for_times(self, *, timestamps_to_search: Mapping[TopicPartition, int]
                          ) -> dict[TopicPartition, OffsetAndTimestamp | None]: ...
    def offsets_for_times(self, *, timestamps_to_search: Mapping[TopicPartition, int],
                          timeout: Duration) -> dict[TopicPartition, OffsetAndTimestamp | None]: ...
    def beginning_offsets(self, *, partitions: Iterable[TopicPartition]) -> dict[TopicPartition, int]: ...
    def beginning_offsets(self, *, partitions: Iterable[TopicPartition],
                          timeout: Duration) -> dict[TopicPartition, int]: ...
    def end_offsets(self, *, partitions: Iterable[TopicPartition]) -> dict[TopicPartition, int]: ...
    def end_offsets(self, *, partitions: Iterable[TopicPartition],
                    timeout: Duration) -> dict[TopicPartition, int]: ...
    def current_lag(self, *, topic_partition: TopicPartition) -> int | None: ...
    def group_metadata(self) -> ConsumerGroupMetadata: ...
    def close(self) -> None: ...
    def close(self, *, option: CloseOptions) -> None: ...
    def wakeup(self) -> None: ...
    # A context manager: leaving the `with` block closes the consumer.
```

On `AsyncConsumer` every method is `async def` except `assignment()`, `subscription()`, `metrics()`, `paused()`, `group_metadata()` and `wakeup()`.

- `poll()` runs the deserializers on the caller's thread. If one fails, `poll()` raises `RecordDeserializationError` and leaves the position at that record: `seek(partition=e.topic_partition(), offset=e.offset() + 1)` skips it.
- `wakeup()`, from any thread, makes the waiting call raise `WakeupError`.
- Pattern subscription takes a `SubscriptionPattern`, which the broker matches (RE2/J). Java's overloads that take a `java.util.regex.Pattern` match on the client and are not translated.
- `enforce_rebalance` is not in the Python client: under KIP-848, Java's consumer only logs that it is not supported.
- Java's `clientInstanceId`, `registerMetricForSubscription` and `unregisterMetricFromSubscription`, and the share consumer (KIP-932), are not in the Python client: the C API has no entry point for them.

The application uses `KafkaConsumer[K, V]`. A deserializer you leave out comes from the config key, else it is the bytes deserializer:

```python
class KafkaConsumer(Consumer[K, V]):
    def __init__(self: KafkaConsumer[bytes, bytes], *, configs: dict[str, Any]) -> None: ...
    def __init__(self: KafkaConsumer[K, bytes], *, configs: dict[str, Any],
                 key_deserializer: Deserializer[K]) -> None: ...
    def __init__(self: KafkaConsumer[bytes, V], *, configs: dict[str, Any],
                 value_deserializer: Deserializer[V]) -> None: ...
    def __init__(self, *, configs: dict[str, Any], key_deserializer: Deserializer[K],
                 value_deserializer: Deserializer[V]) -> None: ...
```

The `ConsumerRecord` looks like this:

```python
class ConsumerRecord(Generic[K, V]):
    def __init__(self, *, topic: str, partition: int, offset: int,
                 key: K | None, value: V | None) -> None: ...
    # ... and Java's two longer constructors, with the timestamp, sizes, headers,
    # leader epoch and delivery count.
    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def headers(self) -> Headers: ...
    def key(self) -> K | None: ...
    def value(self) -> V | None: ...
    def offset(self) -> int: ...
    def timestamp(self) -> int: ...
    def timestamp_type(self) -> TimestampType: ...
    def serialized_key_size(self) -> int: ...
    def serialized_value_size(self) -> int: ...
    def leader_epoch(self) -> int | None: ...
    def delivery_count(self) -> int | None: ...
```

And `ConsumerRecords`, which `poll()` returns, looks like this:

```python
class ConsumerRecords(Generic[K, V]):
    EMPTY: ClassVar[ConsumerRecords[Any, Any]]
    # Java's deprecated ConsumerRecords(Map) constructor is not translated.
    def __init__(self, *, records: Mapping[TopicPartition, Sequence[ConsumerRecord[K, V]]],
                 next_offsets: Mapping[TopicPartition, OffsetAndMetadata]) -> None: ...
    def records(self, *, partition: TopicPartition) -> list[ConsumerRecord[K, V]]: ...
    def records(self, *, topic: str) -> list[ConsumerRecord[K, V]]: ...
    def next_offsets(self) -> dict[TopicPartition, OffsetAndMetadata]: ...
    def partitions(self) -> set[TopicPartition]: ...
    def __iter__(self) -> Iterator[ConsumerRecord[K, V]]: ...
    def __len__(self) -> int: ...  # Java's count()
    def is_empty(self) -> bool: ...
    @staticmethod
    def empty() -> ConsumerRecords[Any, Any]: ...
```

The rebalance listener is an abstract class. Java's abstract methods stay abstract, so a subclass that misses one fails when it is built. Java's `default` method keeps its body:

```python
class ConsumerRebalanceListener(ABC):
    @abstractmethod
    def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None: ...
    @abstractmethod
    def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None: ...
    def on_partitions_lost(self, partitions: set[TopicPartition]) -> None:
        self.on_partitions_revoked(partitions)
```

### Admin interface

The equivalent of the Java `Admin` interface is the base class `Admin`. Applications get a `KafkaAdminClient` from `Admin.create`, as in Java, and tests use `MockAdminClient`. Every operation follows the same shape as `create_topics`:

```python
class Admin:
    @staticmethod
    def create(*, configs: dict[str, Any]) -> Admin: ...
    def create_topics(self, *, new_topics: Iterable[NewTopic]) -> CreateTopicsResult: ...
    def create_topics(self, *, new_topics: Iterable[NewTopic],
                      options: CreateTopicsOptions) -> CreateTopicsResult: ...
    # ... every other operation the same way ...
    def close(self) -> None: ...
    def close(self, *, timeout: Duration) -> None: ...
    # A context manager: leaving the `with` block closes the client.
```

- Each operation returns its `*Result` at once. The result holds one `KafkaFuture` per key (for example one per topic), or one for the whole operation. `all()` gives one future for every key.
- `AsyncAdmin.create` returns an `AsyncKafkaAdminClient`. Its operations are plain `def` too, and its results hold `AsyncKafkaFuture`s.
- The sync client completes its futures on one background thread, as Java's admin thread does.
- Operations that have no entry point in the C API are not generated. The docstring of `confluent_kafka.admin.admin` lists them.

## Changes from the librdkafka-based client

The Python client is the next major version of `confluent-kafka`, and its API is the Java client's. So code written for the librdkafka-based client needs changes. The [Client APIs](README.md#migration) RFC describes the migration guides and tooling. The main changes are:

| librdkafka-based client | This client |
|---|---|
| `Producer(conf)`, `Consumer(conf)`, `AdminClient(conf)` | `KafkaProducer(configs=…)`, `KafkaConsumer(configs=…)`, `Admin.create(configs=…)` |
| librdkafka config keys, e.g. `message.max.bytes` | Java config keys, e.g. `max.request.size` |
| positional arguments | keyword-only arguments |
| `producer.produce(topic, value, key, on_delivery=cb)` | `producer.send(record=ProducerRecord(topic=…, key=…, value=…), callback=cb)` |
| delivery callback `cb(err, msg)`, run by `producer.poll()` or `flush()` | `cb(*, metadata, exception, **kwargs)`, run on the producer's completion thread; there is no producer `poll()` |
| `consumer.poll()` returns one `Message` or `None`; check `msg.error()` | `consumer.poll(timeout=…)` returns a `ConsumerRecords` batch; a failure raises |
| `subscribe(topics, on_assign=…, on_revoke=…)` | `subscribe(topics=…, callback=listener)`, with a `ConsumerRebalanceListener` subclass |
| `commit()` does not wait by default (`asynchronous=True`) | `commit()` waits (Java's `commitSync`); `commit_nowait()` does not |
| `KafkaException` holding a `KafkaError` with `code()` and `retriable()` | one exception class per Java exception (`except RetriableError`); `KafkaError` is the base class |
| `create_topics()` returns a `dict` of `concurrent.futures.Future` | `create_topics()` returns a `CreateTopicsResult` of `KafkaFuture`s; a failure raises `ExecutionError` |

## More comprehensive examples

These are complete, runnable examples, which you can also find in the [`python/examples`](../../python/examples) directory.

### Producer example

This producer reads lines from stdin and sends each one to a topic. A delivery callback prints where each record landed.

```python
import functools
import sys

from confluent_kafka.common.serialization import string_serializer
from confluent_kafka.producer import KafkaProducer, ProducerRecord


def delivery_callback(record, *, metadata, exception, **kwargs):
    """Per-message delivery callback, run on the producer's background
    completion thread once the message is delivered or has permanently failed
    (after retries). The client passes metadata= and exception= (and any parameter
    Java adds later, into **kwargs); the record is
    bound in front by functools.partial, since the metadata carries no value."""
    if exception is not None:
        sys.stderr.write('%% Message failed delivery: %s: %s\n' % (record.value(), exception))
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d: %s\n' %
                         (metadata.topic(), metadata.partition(), metadata.offset(), record.value()))


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    # Producer configuration.
    # The keys are the Java client's: https://kafka.apache.org/documentation/#producerconfigs
    conf = {'bootstrap.servers': broker}

    # Create the producer. The value serializer encodes each line as UTF-8.
    p = KafkaProducer(configs=conf, value_serializer=string_serializer())

    try:
        # Read lines from stdin and send each one (without its newline).
        # send() does not raise when the local buffer is full: it waits for
        # space, up to max.block.ms.
        for line in sys.stdin:
            record = ProducerRecord(topic=topic, value=line.rstrip())
            p.send(record=record, callback=functools.partial(delivery_callback, record))

        # Wait until every message has been delivered or has failed.
        sys.stderr.write('% Waiting for outstanding deliveries\n')
        p.flush()

    finally:
        p.close()
```

### Consumer example

This consumer subscribes to topics, prints every record it reads, and prints every change to its assignment. A Ctrl+C stops it, and `close()` commits the final offsets and leaves the group.

```python
import sys

from confluent_kafka.common.serialization import string_deserializer
from confluent_kafka.consumer import ConsumerRebalanceListener, KafkaConsumer


def partition_list(partitions):
    return ', '.join(str(p) for p in sorted(partitions, key=lambda p: (p.topic(), p.partition()))) or '(none)'


class PrintRebalance(ConsumerRebalanceListener):
    """Prints each change to the consumer's assignment. The methods run on the
    thread calling poll(), and the rebalance waits for them to return."""

    def __init__(self, *, consumer):
        self._consumer = consumer

    def on_partitions_assigned(self, partitions):
        # A listener may call back into its consumer.
        sys.stderr.write('%% Assigned: %s; assignment now: %s\n' %
                         (partition_list(partitions), partition_list(self._consumer.assignment())))

    def on_partitions_revoked(self, partitions):
        sys.stderr.write('%% Revoked: %s\n' % partition_list(partitions))

    def on_partitions_lost(self, partitions):
        sys.stderr.write('%% Lost: %s\n' % partition_list(partitions))


if __name__ == '__main__':
    if len(sys.argv) < 4:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    group = sys.argv[2]
    topics = sys.argv[3:]

    # Consumer configuration.
    # The keys are the Java client's: https://kafka.apache.org/documentation/#consumerconfigs
    conf = {
        'bootstrap.servers': broker,
        'group.id': group,
        # This client speaks only the KIP-848 consumer group protocol.
        'group.protocol': 'consumer',
        'auto.offset.reset': 'earliest',
        # With auto-commit (the default), the consumer commits the offsets of
        # the records poll() returned every auto.commit.interval.ms (5 s),
        # before a rebalance takes partitions away, and on close(). Processing
        # each batch before the next poll() gives at-least-once delivery.
    }

    # Create the consumer. The deserializers decode keys and values as UTF-8.
    c = KafkaConsumer(configs=conf, key_deserializer=string_deserializer(),
                      value_deserializer=string_deserializer())

    # Subscribe to the topics.
    c.subscribe(topics=topics, callback=PrintRebalance(consumer=c))

    # Read messages from Kafka, print to stdout.
    try:
        while True:
            # poll() returns a batch of records, possibly empty. A failed
            # poll() raises a KafkaError subclass.
            records = c.poll(timeout=1.0)
            for msg in records:
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
                print(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('% Aborted by user\n')

    finally:
        # Close the consumer: it commits the final offsets and leaves the group.
        c.close()
```

### Asyncio consumer example

The same consumer on asyncio. The listener's methods are coroutines, which the consumer awaits on the event loop. Leaving the `async with` block closes the consumer.

```python
import asyncio
import sys

from confluent_kafka.common.serialization import string_deserializer
from confluent_kafka.consumer import AsyncKafkaConsumer, ConsumerRebalanceListener


def partition_list(partitions):
    return ', '.join(str(p) for p in sorted(partitions, key=lambda p: (p.topic(), p.partition()))) or '(none)'


class PrintRebalance(ConsumerRebalanceListener):
    """Prints each change to the consumer's assignment. On the async consumer
    the methods may be coroutines: the consumer awaits them on the event loop,
    and the rebalance waits for them to finish."""

    def __init__(self, *, consumer):
        self._consumer = consumer

    async def on_partitions_assigned(self, partitions):
        # A listener may call back into its consumer.
        sys.stderr.write('%% Assigned: %s; assignment now: %s\n' %
                         (partition_list(partitions), partition_list(self._consumer.assignment())))

    async def on_partitions_revoked(self, partitions):
        sys.stderr.write('%% Revoked: %s\n' % partition_list(partitions))

    async def on_partitions_lost(self, partitions):
        sys.stderr.write('%% Lost: %s\n' % partition_list(partitions))


async def main(broker, group, topics):
    # Consumer configuration.
    # The keys are the Java client's: https://kafka.apache.org/documentation/#consumerconfigs
    conf = {
        'bootstrap.servers': broker,
        'group.id': group,
        # This client speaks only the KIP-848 consumer group protocol.
        'group.protocol': 'consumer',
        'auto.offset.reset': 'earliest',
        # With auto-commit (the default), the consumer commits the offsets of
        # the records poll() returned every auto.commit.interval.ms (5 s),
        # before a rebalance takes partitions away, and on close(). Processing
        # each batch before the next poll() gives at-least-once delivery.
    }

    # The deserializers decode keys and values as UTF-8.
    async with AsyncKafkaConsumer(configs=conf, key_deserializer=string_deserializer(),
                                  value_deserializer=string_deserializer()) as c:
        await c.subscribe(topics=topics, callback=PrintRebalance(consumer=c))

        # Read messages from Kafka, print to stdout.
        while True:
            # poll() returns a batch of records, possibly empty. A failed
            # poll() raises a KafkaError subclass.
            records = await c.poll(timeout=1.0)
            for msg in records:
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
                print(msg.value())
    # Leaving the block, a Ctrl+C included (asyncio.run cancels this task),
    # closed the consumer: it committed the final offsets and left the group.


if __name__ == '__main__':
    if len(sys.argv) < 4:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % sys.argv[0])
        sys.exit(1)

    try:
        asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3:]))
    except KeyboardInterrupt:
        sys.stderr.write('% Aborted by user\n')
```

### Admin example

This admin client creates a topic, waits for the result of each topic, then lists the topics. A topic that already exists is reported, not raised.

```python
import sys

from confluent_kafka import ExecutionError
from confluent_kafka.admin import Admin, NewTopic
from confluent_kafka.common.errors import TopicExistsError

if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]

    # Admin configuration.
    # The keys are the Java client's: https://kafka.apache.org/documentation/#adminclientconfigs
    conf = {'bootstrap.servers': broker}

    with Admin.create(configs=conf) as admin:
        # create_topics() returns its result at once; the result holds one
        # KafkaFuture per topic.
        result = admin.create_topics(
            new_topics=[NewTopic(name=topic, num_partitions=3, replication_factor=1)])

        # get() waits for a topic and raises ExecutionError for a failed one,
        # the error being its __cause__.
        for name, future in result.values().items():
            try:
                future.get()
                print('Topic %s created' % name)
            except ExecutionError as e:
                if not isinstance(e.__cause__, TopicExistsError):
                    raise
                print('Topic %s already exists' % name)

        # list_topics() returns one future with every topic's name.
        print('Topics: %s' % sorted(admin.list_topics().names().get()))
```
