# Changelog

All notable changes to the Node.js client (`confluent-kafka-javascript-rust`,
a working name) are documented in this file.

Before GA the public API is not stable, and an interface may change even in a
minor release.

## [Unreleased]

Not published: the package is `private`.

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

### Package

- The root exports the module namespaces `{ producer, common }`; each is also
  an entry point (`<package>/producer`, `<package>/common`) with its own
  TypeScript declarations, generated from the JSDoc.
- Installing runs no build (`"gypfile": false`, no install scripts).
