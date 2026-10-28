# Changelog

All notable changes to the `confluent-kafka` Python package are documented in this file.

Before GA the public API is not stable, and an interface may change even in a minor release.

## [Unreleased]

First preview release of the 4.4.0 Python client built on a new Rust core. Its
API is the Apache Kafka Java client's (4.4.0), in Python form.

> **Preview:** not yet recommended for production use. The public API is not
> stable before the GA release and may change. We welcome your feedback while
> the design is still open.

### Clients

- Producer, consumer and admin client, each as a sync class (`KafkaProducer`,
  `KafkaConsumer`, `KafkaAdminClient`) and an asyncio peer
  (`AsyncKafkaProducer`, `AsyncKafkaConsumer`, `AsyncKafkaAdminClient`).
- Mock clients for tests (`MockProducer`, `MockConsumer`, `MockAdminClient`),
  each with an asyncio peer.

### Producer

- Send with a callback and a future for the record's metadata; flush and close.
- Idempotent producer and transactions.

### Consumer

- KIP-848 consumer group protocol: subscribe to topics or to a broker-side
  pattern, or assign partitions manually.
- Rebalance listeners, which can commit, seek and query the consumer from
  inside the callback.
- Offset commits (waiting and not waiting), seek and offset lookups, pause and
  resume, and `wakeup()`.

### Admin client

- Topics and partitions, configurations, consumer groups and offsets, ACLs,
  quotas, SCRAM credentials, delegation tokens, transactions, features and log
  directories. Each operation returns its result at once, with one future per
  key, as in Java.

### Common

- Java's API in Python form: snake_case names, keyword-only parameters, one
  method per Java overload set.
- Typed errors, one class per Java exception, caught by class
  (`except RetriableError`).
- One future type, `KafkaFuture` / `AsyncKafkaFuture`, for every future the
  client hands out.
- Built-in serializers and deserializers (bytes, string, int, float, bool,
  UUID, JSON); any callable can be a custom one.
- Configuration with the Java client's keys.
- Full type hints (`py.typed` and stubs), checked with `mypy --strict`.
- Prebuilt wheels for Python 3.10 to 3.14 on Linux (x86-64, arm64) and macOS
  (x86-64, arm64).
- Each client logs a preview warning when it is constructed.

### Known limitations

- Not compatible with the API of the librdkafka-based `confluent-kafka` 2.x
  package. Both install the same top-level `confluent_kafka` module, so they
  cannot share one environment.
- Only the KIP-848 consumer group protocol: `group.protocol=classic` is
  rejected.
- The share consumer (KIP-932) is not included.
- Limited pluggable interfaces.
- Of the SASL mechanisms, only `PLAIN` is supported. Keys and certificates must
  be PEM (no JKS or PKCS12).
- No Windows wheels.
- Free-threaded Python (3.13t, 3.14t) is not supported yet: no wheels are built
  for it, and a build from source turns the GIL back on when the module is
  imported.
