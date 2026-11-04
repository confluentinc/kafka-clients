# Changelog

All notable changes to the Node.js client (`@confluentinc/kafka-javascript`)
are documented in this file.

Before GA the public API is not stable, and an interface may change even in a
minor release.

## [4.4.0-beta.1] - Unreleased

Not published: the package is `private`. It takes the npm name of the
librdkafka-based client, `@confluentinc/kafka-javascript`, and is told apart
from it by version, as the Rust-based Python client reuses PyPI's
`confluent-kafka` at `4.4.0b1`: `4.4.0-beta.1` is npm's form of that version
(the existing package's releases are 0.x and 1.x). Publishing under the shared
name needs the agreement of the team that owns it.

### Producer

- `KafkaProducer`, `ProducerRecord`, `RecordMetadata`, Java's `Callback` and,
  from `common`, `KafkaFuture` and `KafkaError`, over the Rust client's C FFI.
- `send(record, callback?)` resolves at hand-over with a `KafkaFuture` whose
  `get()` settles at delivery; `flush()` and `close({timeoutMs}?)`.
- A producer whose `close()` never completed is force-closed (Java's
  `close(Duration.ZERO)`) when it is garbage collected or its worker or
  process ends: its unsent records fail, and no JS callback or future runs for
  them. `await producer.close()` before teardown when records must be
  delivered.

### Errors and security

- The error classes of Java's client, generated from the Java sources as the
  Python client's are: Java's names (`Exception` read as `Error`) and
  `extends` chain, in the module of their Java package (`common/errors`, ...),
  the Java built-in errors (`IllegalStateError`, ...) at the root. An error
  the client reports is an instance of the class of its code, with `name` (the
  class name), `code`, `errorName` (the code's enumerator name), `message`,
  `retriable`, `apiError` and `cause`.
- `ProducerConfig`, `SslConfigs`, `SaslConfigs` and `SecurityConfig` constants,
  generated from the Java sources; `SslClientAuth` and `SecurityProtocol`.
- A config value is checked against its key's Java `ConfigDef` type, and a
  `null` against its key's validator, before the core sees the configuration
  (`ConfigError` with Java's message; a password's value is never shown).

### Known limitations

- Of the SASL mechanisms, only `PLAIN` is supported. Keys and certificates must
  be PEM (no JKS or PKCS12).
- The error classes take `(message?, {cause}?)`, not Java's constructors, and
  have none of Java's payload getters (`TopicAuthorizationError`'s
  `unauthorizedTopics()`, `RecordTooLargeError`'s
  `recordTooLargePartitions()`, ...), the `INSTANCE` singletons
  (`DisconnectError`, `CoordinatorNotAvailableError`) or
  `RecordDeserializationError.DeserializationExceptionOrigin`.

### Package

- The root exports the module namespaces (`producer`, `common`, `consumer`)
  and the Java built-in errors; each namespace, nested ones included
  (`<package>/common/errors`), is also an entry point with its own TypeScript
  declarations.
- Written in TypeScript and published compiled (`dist/`: CommonJS, which
  `require` loads and an ES module `import`s, with its `.d.ts` declarations
  and source maps).
- Installing runs no build (`"gypfile": false`, no install scripts).
