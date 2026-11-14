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
Java's `long` as a `bigint` (`metadata.offset`, `metadata.timestamp`,
`record.timestamp`), keys and values borrowed until `send()` settles, a producer keeping Node running until it is closed, no garbage-collection close, and Node.js 22.14 or
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
- Java's `Producer` interface, a TypeScript interface that `KafkaProducer`
  implements. `KafkaFuture` is an abstract class, as Java's is: constructing
  it throws `TypeError`, and a subclass may be built. `DisconnectError` and
  `CoordinatorNotAvailableError` have Java's `INSTANCE`. `SslConfigs`,
  `SaslConfigs` and `SecurityConfig` keep Java's public constructor.
- `ProducerRecord` and `RecordMetadata` are data objects: Java's getters are
  public `readonly` fields (`metadata.offset`, `metadata.hasOffset`), which a
  spread and `JSON.stringify` keep (`structuredClone` / `postMessage` keep
  every field but a record's `headers`, which becomes `{}`). A `ProducerRecord`
  is frozen, so `send()` sends the fields its constructor checked. `SecurityProtocol` and
  `SslClientAuth` have Java's `Enum` methods, `name()`, `ordinal()`,
  `values()` and `valueOf()`; `toString()` and `toJSON()` return the name
  (`SslClientAuth`'s too, where Java's writes it in lower case). A method that
  only reads throws `TypeError` when it is given an argument.
  `RecordMetadata` and the enums have private constructors.
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
- The app's `Buffer` / `Uint8Array` key and value are not copied: the client
  reads them after `send()` returns, so don't write to them, transfer them or
  resize them until the Promise from `send()` settles (awaiting `send()` is
  enough), and a serializer must return a new Buffer for each record. The
  client holds a reference on each until then. A string key or value is encoded to
  UTF-8 as Java's `StringSerializer` encodes it, a lone surrogate as `?` (Node
  writes U+FFFD), and so are the header keys and the config keys and values;
  topic names are not.
- `send({ record, callback })` (or `send(record)`) resolves at hand-over with a
  `KafkaFuture` whose `get()` settles at delivery; `flush()` and `close()` /
  `close(timeout)`, a timeout being milliseconds or a `Temporal.Duration`
  (`RangeError` for one with years, months or weeks; `CloseArgs` is the type
  of `close`'s options). A call after `close()` goes to the client, which
  answers as Java's does: `send()` rejects with `IllegalStateError`, and
  `flush()` and a second `close()` resolve. Each Java overload has its own
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
- A producer keeps Node running from its creation until `close()` closes it,
  idle or not, as KafkaJS and 1.x do from `connect()` to `disconnect()`, and
  no garbage collection closes one: a script that never calls `close()` never
  exits on its own. Once closed, a producer no longer holds Node, and its
  native client is released when it is garbage collected.

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
- The client has no `ConfigDef` table and checks no config value: every
  value is written as a string (`true` as `"true"`, numbers and bigints in
  decimal, arrays joined with `,`, anything else by `String()`), `null` and
  `undefined` leave the key unset, and the core checks the keys and values
  with Java's messages naming the key. Unknown keys, a `kafkaJS` block
  included, reach the core, which lists the ones it does not use;
  `key.serializer` and `value.serializer` throw `ConfigError` pointing to the
  constructor arguments. The `ProducerConfigs` type lists Java's producer keys
  for autocompletion and takes any other string.
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
