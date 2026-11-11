# Changelog

All notable changes to the Node.js client (`@confluentinc/kafka-javascript`)
are documented in this file.

Before GA the public API is not stable, and an interface may change even in a
minor release.

## [4.4.0-preview.1] - Unreleased

Not published yet. It takes the npm name of the librdkafka-based client,
`@confluentinc/kafka-javascript`, and is told apart from it by version, as the
Rust-based Python client reuses PyPI's `confluent-kafka` at `4.4.0b1` (the
existing package's releases are 0.x and 1.x). Publishing under the shared name
needs the agreement of the team that owns it.

### Changes from the development builds

The call shape of the earlier development builds changed; the README's
"Migrating from the earlier development builds" lists every change. In short:
one object of Java-named parameters for every overload set (or the value on
its own for a one-parameter one), `send({ record, callback })`, a callback of
`({ metadata, exception })`, `close(timeout)` with a `Duration`, exactly
Java's `ProducerRecord` constructors, `undefined` refused (`close(undefined)`
included), a bare value of no overload's type refused with
`IllegalArgumentError`, `Header` / `Headers` classes (`header.key()`,
`header.value()`, no `[key, value]` pairs), `readonly` data fields, each
package imported from its own path (the root's namespaces are gone), `ExecutionError` from a failed
`get()`, errors without `code` / `errorName` / `retriable` / `apiError`,
copied keys and values, no garbage-collection close, and Node.js 22.14 or
newer.

### Packaging

- Released as three npm packages: `@confluentinc/kafka-javascript`, the
  compiled library, and a platform package per platform of the preview,
  `@confluentinc/kafka-javascript-linux-x64-gnu` (glibc) and
  `-darwin-arm64`, each carrying the addon with the Rust client linked
  statically, which npm installs from the main package's optional
  dependencies, pinned at its exact version, with no download fallback. No
  package builds or runs a script at install. Linux arm64 has no platform
  package in the preview: build from source there.
- Every package ships a `NOTICE` with Apache Kafka's credit line, and each
  platform package `THIRD_PARTY_LICENSES.txt`, the licences of the Rust
  crates linked into its addon.
- Pre-releases are published under the `preview` dist-tag
  (`npm install @confluentinc/kafka-javascript@preview`); `latest`, `rc` and
  `alpha` stay with the earlier client; the publish job refuses to put a
  pre-release on `latest`. Semaphore publishes, with a granular npm token and
  no provenance. `-preview.N` is a pre-release label of this package.
- Node.js 22.14 or newer (`engines`), tested on 22, 24 and 26.
- `MIGRATION.md`, shipped in the package, is the migration guide to 4.x for
  users of 1.x (both of its APIs) and of KafkaJS: what replaces what, what is
  not offered, what is not available yet and what changes silently, first of
  all `await send()` (accepted, not delivered) and the default partitioner
  (CRC-32; `partitioner.type: 'Murmur2RandomPartitioner'` keeps KafkaJS's
  murmur2 partitions).
- The published package has the folders behind its import paths
  (`producer/`, `common/config/`, ...), each with a `package.json` pointing
  into `dist/`, for TypeScript setups that do not read `exports`; a deep
  import fails with `ERR_PACKAGE_PATH_NOT_EXPORTED`.
- The interface is checked against a committed API report of the compiled
  declarations and the `exports` map (`api/`): before GA a change warns.
- While the client is in preview, the first client a process builds emits one
  `ExperimentalWarning`, which `--no-warnings` silences. The binding's debug
  lines are shown with `NODE_DEBUG=confluent_kafka`.
- The addon is loaded from the platform package, or else from the development
  build, `native/build/Release/`; when neither loads, the error names the platform,
  the supported ones and how to install or build for it.

### Producer

- `KafkaProducer`, `ProducerRecord`, `RecordMetadata`, Java's `Callback` and,
  from `common`, `KafkaFuture` and `KafkaError`, over the Rust client's C FFI.
- `ProducerRecord` and `RecordMetadata` are data objects: Java's getters are
  public `readonly` fields (`metadata.offset`, `metadata.hasOffset`), which a
  spread and `JSON.stringify` keep (`structuredClone` / `postMessage` keep
  every field but a record's `headers`, which becomes `{}`). `SecurityProtocol` and
  `SslClientAuth` have Java's `Enum` methods, `name()`, `ordinal()`,
  `values()` and `valueOf()`; `toString()` and `toJSON()` return the name
  (`SslClientAuth`'s too, where Java's writes it in lower case). A method that
  only reads throws `TypeError` when it is given an argument.
  `RecordMetadata`, `KafkaFuture` and the enums have private constructors.
- `Header` and `Headers`, in `common/header`, with Java's members: `Header`
  with Java's methods (`new Header({ key, value })`, `header.key()`,
  `header.value()`, a `Buffer`) and a `toJSON()`, `Headers` with `add`,
  `remove`, `lastHeader`, `headers(key)`, `toArray`, iteration and Java's
  `toString()`. A record's `headers` is its own `Headers`, built from a
  `Headers` or an iterable of `Header`s. `send()` makes the record's headers read-only, as
  Java's does: a later `add` or `remove` throws `IllegalStateError`
  "RecordHeaders has been closed.".
- A header value is bytes (`Uint8Array` or `null`), as in Java and Python: a
  string header value throws `TypeError` (encode it first,
  `Buffer.from(value, 'utf8')`), where development builds took a string.
- The app's `Buffer` / `Uint8Array` key and value are copied during `send()`,
  so they may be changed, reused or detached as soon as it returns; the copy
  is freed when the record completes. A string key or value is encoded to
  UTF-8 as Java's `StringSerializer` encodes it, a lone surrogate as `?` (Node
  writes U+FFFD), and so are the header keys and the config keys and values;
  topic names are not.
- `send({ record, callback })` (or `send(record)`) resolves at hand-over with a
  `KafkaFuture` whose `get()` settles at delivery; `flush()` and `close()` /
  `close(timeout)`, a timeout being milliseconds or a `Temporal.Duration`
  (`RangeError` for one with years, months or weeks; `CloseArgs` is the type
  of `close`'s options). `close()` on a closing or closed producer returns
  that close before it reads its timeout. Each Java overload has its own
  TypeScript signature.
  `KafkaProducer` has `[Symbol.asyncDispose]`, so `await using` closes it.
- The callback is Java's `onCompletion(metadata, exception)` as one object,
  `({ metadata, exception }) => ...`. A callback that returns a Promise is
  awaited (its result is wrapped with `Promise.resolve`): the record's future
  completes, and a `flush()` or `close()` covering the record returns, once
  it settled; a rejection is logged as a throw is, and reaches no caller.
- `KafkaFuture` has Java's members: `completedFuture`, `allOf`,
  `toCompletionStage()` (a `Promise`), `thenApply`, `whenComplete`, `cancel`,
  `get()` and `get(timeout)`, `getNow`, `isCancelled`,
  `isCompletedExceptionally` and `isDone`. A failed send's `future.get()` (and
  `getNow`) rejects with `ExecutionError`, Java's `ExecutionException`, whose
  `cause` is the delivery error, the one the callback receives: check
  `e.cause`. A timed-out `get(timeout)` rejects with the root `TimeoutError`,
  and a cancelled future's `get()` with the root `CancellationError`, Java's
  `CancellationException` (an `IllegalStateError`). A record's future is never
  cancelled (`cancel()` returns `false`); the futures `completedFuture`,
  `allOf`, `thenApply` and `whenComplete` make can be.
- Serializers: `new KafkaProducer({ configs, keySerializer, valueSerializer })`
  takes a `Serializer` (`common/serialization`) for each, or `null`: an
  object `{ serialize({ topic, headers, data }), close?() }`, Java's two
  `serialize` methods as one. They run in `send()`, key then value, are
  called for a null key or value too, and may return a Promise, which
  `send()` awaits (sends not awaited one by one may then reach the producer
  out of call order); what they return is a `Uint8Array` or `null`, copied
  like the app's bytes. The producer's type arguments are inferred from them,
  and a producer without them is a `KafkaProducer<Uint8Array, Uint8Array>`.
  `close()` closes both; a failure there is a process warning. A function
  given where a serializer goes throws `IllegalArgumentError` "... did you
  forget '()'?". `ProducerRecord<K, V>` is generic: its key and value are
  what the serializers take, checked at `send()` when there is no serializer.
- `MockProducer`, a TypeScript translation of Java's: `new MockProducer()` and
  `new MockProducer({ autoComplete, partitioner, keySerializer,
  valueSerializer })` (the form taking a `Cluster` is not translated, so the
  partition is always 0; `Partitioner` is a placeholder type), `send`,
  `flush` and `close` returning Promises as `KafkaProducer`'s do,
  `[Symbol.asyncDispose]`, `history()`, `clear()`, `completeNext()`,
  `errorNext(e)`, `closed()`, `flushed()` and Java's public fields
  `sendException`, `flushException` and `closeException`, an injected error
  being thrown again as the same instance. Its transaction members,
  `partitionsFor`, `metrics` and `setMockMetrics` are not translated yet.
- `KafkaProducer.NETWORK_THREAD_PREFIX` and `PRODUCER_METRIC_GROUP_NAME`, and,
  in `common`, `TopicPartition` (a frozen data object with a `toJSON()`),
  `Node` (a data object) and `PartitionInfo` (Java's methods; no public
  constructor yet).
- No garbage collection closes a producer: one never closed keeps running
  until its worker or the process ends, and is then force-closed (Java's
  `close(Duration.ZERO)`): its unsent records fail, and no JS callback or
  future runs for them. When the event loop is about to empty with such
  producers, one warning (code `CONFLUENT_KAFKA_PRODUCER_NOT_CLOSED`) gives
  their number. `await producer.close()` before teardown when records must be
  delivered.

### Errors and security

- The error classes of Java's client, generated from the Java sources as the
  Python client's are: Java's names (`Exception` read as `Error`) and
  `extends` chain, in the module of their Java package (`common/errors`, ...),
  the Java built-in errors (`IllegalStateError`, ...) at the root,
  `IllegalArgumentError` and `IndexOutOfBoundsError` extending JavaScript's
  `RangeError`. An error the client reports is an instance of the class of its
  Java exception, with `name` (the class name), `message` and `cause` and no
  other public field: classify with `instanceof` (or `name`), not `err.code`
  or `err.retriable` (a retriable error is an `instanceof RetriableError`).
- An error's constructors are Java's forms with a message and a cause, by name
  or, with one parameter, the value on its own: `new KafkaError('boom')`,
  `new KafkaError({ message, cause })`, `new KafkaError(cause)`.
- A `null` the client rejects as Java's `NullPointerException` throws
  `TypeError` "<param> must not be null" (`record`, `configs`, a
  `SecurityProtocol.forName` name); where Java throws its own message
  ("Topic cannot be null.", "Null header keys are not permitted", "Header
  cannot be null.") the client keeps it.
- `ProducerConfig`, `SslConfigs`, `SaslConfigs` and `SecurityConfig` constants,
  generated from the Java sources (a constant `Set` a read-only set, a
  constant `List` a frozen array); `SslClientAuth` and `SecurityProtocol`.
- A config value is checked against its key's Java `ConfigDef` type, and a
  `null` against its key's validator, before the core sees the configuration
  (`ConfigError` with Java's message; a password's value is never shown). An
  unknown key's value is a string, a number or a boolean; `key.serializer`
  and `value.serializer` throw `ConfigError` pointing to the constructor
  arguments, and a `kafkaJS` block one pointing to the migration guide.
  Numbers are written in decimal.
- A second argument to an error constructor throws an `IllegalArgumentError`
  pointing to `{ message, cause }`. An error the client has nowhere else to
  report goes to `process.emitWarning`.
- A topic, a header key, or a config key or value holding a NUL byte throws a
  `TypeError`: C would read the NUL as the end of the string. Keys, values and
  header values are bytes and may hold any.

### Known limitations

- One copy of the package per application: a record or header made by one
  copy is refused by a producer of another, and errors are no `instanceof`
  the other copy's classes.
- A client's objects are not shared between worker threads: one producer per
  thread, and plain data between threads (a record's `Headers` as pairs).
- A default import (`import kafka from '@confluentinc/kafka-javascript'`)
  works only under Node's own ES module loader: compiled to CommonJS by an
  interop helper (TypeScript's `esModuleInterop`, Babel, a bundler) it would
  read a `default` the package does not have, so TypeScript rejects it (TS1192).
  Use `require`, named imports or `import * as`.
- Of the SASL mechanisms, only `PLAIN` is supported. Keys and certificates must
  be PEM (no JKS or PKCS12).
- The error classes take only Java's constructors with a message and a cause;
  those with a payload are not translated, nor are Java's payload getters
  (`TopicAuthorizationError`'s `unauthorizedTopics()`, `RecordTooLargeError`'s
  `recordTooLargePartitions()`, ...), the `INSTANCE` singletons
  (`DisconnectError`, `CoordinatorNotAvailableError`) or
  `RecordDeserializationError.DeserializationExceptionOrigin`. A class whose
  every Java constructor takes a payload (`QuotaViolationError`,
  `RecordDeserializationError`, ...) cannot be constructed yet.

### Package

- Each Java package has its own import path (`<package>/producer`,
  `<package>/common/errors`, ...) with its own TypeScript declarations, and no
  index re-exports another; the root exports only the Java built-in errors and
  the `Duration` type.
- Written in TypeScript and published compiled (`dist/`: CommonJS, which
  `require` loads and an ES module `import`s, with its `.d.ts` declarations
  and source maps).
- Installing runs no build (`"gypfile": false`, no install scripts).
