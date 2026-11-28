# Changelog

All notable changes to the `confluent-kafka` Python package are documented in this file.

Before GA the public API is not stable, and an interface may change even in a minor release.

## [4.4.0b2.dev1] - 2026-11-27

### Security

- Without `ssl.truststore.location` or `ssl.truststore.certificates`, the
  broker certificate is verified by the operating system's certificate
  verifier (its root certificates on Linux, where `SSL_CERT_FILE` /
  `SSL_CERT_DIR` override them), as the Java client uses the JDK's default
  trust store (the core's `platform-verifier` feature); the wheel no longer
  ships Mozilla's root bundle. On Linux a host without a CA bundle must set
  one of the truststore keys or `SSL_CERT_FILE`.

### Changed

- A `None` for an argument Java requires reaches the Rust core, which answers
  with Java's error and message: a `NullPointerException` is Python's
  `TypeError`, with the core's message and its "null" spelled "None"
  (`pause(partitions=None)` -> `TypeError("The partitions to pause must be
  nonnull")`), and an `IllegalArgumentError` keeps Java's text
  (`subscribe(topics=None)` -> "Topic collection to subscribe to cannot be
  null"). The consumer, producer and admin client no longer check these
  arguments in Python first, so some messages differ from the previous release.
  The admin client still checks a value that crosses as its fields or by value
  (`describe_acls(filter=None)`, `renew_delegation_token(hmac=None)`).
- The producer copies a record once on its way to the socket instead of
  twice, and no longer zero-fills the buffers it allocates, so it uses less
  memory and CPU per byte sent: the core's finished batch is a view of its
  pooled buffer, as Java's is.

### Fixed

- `KafkaConsumer` and `AsyncKafkaConsumer` no longer release their rebalance
  listener at `subscribe(topics=[])`. That call is Java's `unsubscribe()`,
  which keeps the listener, so a later `close()` called a released listener:
  the sync consumer crashed the process and the asyncio one failed with a
  `LocalCallbackError`. The listener now stays registered, and a new
  `callback` given with the empty `topics` is ignored, as in Java. A failed
  `subscribe()` keeps its listener alive too, since the core may still call
  it (a `MockConsumer`'s `rebalance()` after `assign()`).

### Fixed

- A `str` argument holding a NUL character raises `ValueError("embedded null
  character")` where it crosses to the Rust core as a C string, as a record's
  topic and the producer's and the consumer's configs already did. It reached
  the core cut at the NUL: `subscribe(topics=["orders\0-test"])` subscribed to
  `orders` and `delete_topics()` deleted it, an admin client config key
  `"security.protocol\0"` replaced the application's `security.protocol`, and
  the same went for the admin client's group ids, transactional ids, user
  names and group instance ids, its option strings, a `CreateTopicsResult`
  accessor's topic and committed offset metadata. Like a non-`str`'s
  `TypeError`, the error comes before the core's own checks (a missing
  `group.id`, a producer without `transactional.id`).

## [4.4.0b1] - 2026-10-30

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
- Windows is not supported yet.
- Free-threaded Python (3.13t, 3.14t) is not supported yet: no wheels are built
  for it, and a build from source turns the GIL back on when the module is
  imported.
- On Python 3.14, a Ctrl+C (`KeyboardInterrupt`) can, rarely, leave a client
  call counted, so that a later `close()` waits forever, or leave a mock
  client's lock held. It happens when the Ctrl+C arrives right after a `with`
  block takes its lock and before the block starts. The bug is in CPython 3.14
  and affects every `with` block
  ([python/cpython#148874](https://github.com/python/cpython/issues/148874));
  Python 3.15 has the fix.
