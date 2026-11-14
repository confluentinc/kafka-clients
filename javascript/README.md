# Confluent Kafka JavaScript (Node.js) client — preview

A Node.js client for Apache Kafka built on the C FFI of the Confluent Kafka
Rust client, in the same way as the Python client in `../python`. It is a
Node-API addon written in plain C (`native/`), targeting Node-API version 9,
so one build works on every Node release that supports it; the package itself
needs Node.js 22.14 or later (`engines`). The addon sits under a thin layer
written in TypeScript (`src/`), compiled into CommonJS with its type
declarations (`dist/`), so it is used from CommonJS (`require`), from ES
modules (`import`) and from TypeScript alike (see "Layout" for the import
forms).

Moving from 1.x (either of its APIs) or from KafkaJS: see
[`MIGRATION.md`](MIGRATION.md), the migration guide to 4.x, which lists what
replaces what, what is not offered and what changes silently (first of all,
`await send()` now means accepted, not delivered, and the default partitioner
hashes keys with CRC-32).

The API follows the Java client's shape (`KafkaProducer`, `ProducerRecord`,
`RecordMetadata`, `KafkaFuture`), with JavaScript idioms (a `Promise` for a
blocking call, a `KafkaFuture` whose `get()` returns a `Promise`). This first
slice covers the producer only.

The package is `@confluentinc/kafka-javascript`, the npm name of the
librdkafka-based client, at version `4.4.0-beta.1`: like the Rust-based Python
client, which reuses PyPI's `confluent-kafka` at `4.4.0b1`, it is told apart
from the earlier client (0.x and 1.x releases) by version, 4.0.0 or higher.
Its pre-releases are published under npm's `beta` dist-tag, never `latest`,
`rc` or `alpha`, which the earlier client's 1.x releases hold. The npm 1.x
releases are built from the `2.x` branch of
[confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript);
report a 1.x bug against that branch (the issue form asks which version). While it is in
preview, the first client a process builds emits one `ExperimentalWarning`;
`node --no-warnings` silences it. The binding's debug lines (the producer's
creation, close and callback pump, the addon it loads) are shown with
`NODE_DEBUG=confluent_kafka`; the core logs on its own under `RUST_LOG`. The
repository's `package.json` stays `private`; the release pipeline writes the
published one (see "Packaging").

## Install from npm

```bash
npm install @confluentinc/kafka-javascript@beta
```

Prebuilt for Linux x64 with glibc 2.28 or newer (RHEL 8, Debian 10, Ubuntu
20.04 and later) and for macOS 11 or newer on Apple silicon, on Node.js 22.14
or newer (tested on 22, 24 and 26). The main package carries the compiled
library only; npm installs the one platform package of the machine,
`@confluentinc/kafka-javascript-linux-x64-gnu` or `-darwin-arm64`, which the
main package pins at its own version in its optional dependencies. Each
carries the addon with the Rust client linked in, so nothing is built at
install time, nothing is downloaded, and `--ignore-scripts` changes nothing; do
not install with `--omit=optional`. Installing for another machine (a
container image built on a laptop) needs npm's `--os`, `--cpu` and `--libc` of
that machine, and so does a lockfile made on one OS and installed on another:
without them it has no entry for the other machine's platform package. A
missing platform package fails at load with an error that names the package
and these causes. Linux arm64, musl (Alpine) and Windows have no platform package
in the preview: build from source there.

## Requirements

- Node.js 22.14 or newer, with its headers (official tarballs, Homebrew and Nix
  installs ship them under `<prefix>/include/node`).
- A C compiler and Python 3 (for node-gyp). node-gyp itself is the copy npm
  bundles.
- The devDependencies, from `package-lock.json` (TypeScript and `@types/node`
  compile the library; the gRPC packages serve the multilanguage suite below):

  ```bash
  (cd javascript && npm ci)
  ```
- The Rust library and its bindings header:

  ```bash
  make -C rust build-all-features   # rust/target/release/libconfluent_kafka.*
                                    # rust/target/include/confluent_kafka_bindings.h
  ```

## Build and test

```bash
(cd javascript && npm ci)            # once: typescript, @types/node and the gRPC packages
make -C javascript build             # the addon (node-gyp) and the library (tsc, src/ -> dist/);
                                     # or, from the repository root: make build-javascript
make -C javascript test-static       # header conformance and package layout, no build needed
make -C javascript test-unit         # no broker needed
make -C javascript test-typing       # dist/'s declarations type-check under --strict
make -C javascript check-api         # the interface check: dist/'s declarations and the exports
                                     # map against the API report in api/ (update-api records it)
make -C javascript test-integration  # starts a scratch broker with docker, or uses
                                     # KAFKA_BOOTSTRAP_SERVERS when it is set
```

`make test` runs all four, then, on Linux, the container arm of the Rust
multilanguage suite (below; it is skipped with a message elsewhere). Every
target that runs tests but `test-static` builds first: the tests, the example
and the gRPC server are JavaScript run against the compiled `dist/`.
`make build-addon` and `make build-ts` build either half. `TSC` (default
`node_modules/.bin/tsc`) points at another TypeScript compiler, and
`TS_TYPE_ROOTS` (default `node_modules/@types`) at the `@types/node` the typing
tests use; without a runnable `tsc` the build fails, telling you to run
`npm ci`.

`CONFLUENT_KAFKA_LIB_DIR` (default `../rust/target/release`) and
`CONFLUENT_KAFKA_INCLUDE_DIR` (default `../rust/target/include`) select the
library and header.

`CONFLUENT_KAFKA_LINK` (`native/binding.gyp`) chooses how the addon links the library:

- `dynamic`, the default, for development, the tests and the gRPC image: it
  links `libconfluent_kafka.{so,dylib}` with an rpath to that directory.
- `static`, for a release: it links `libconfluent_kafka.a` into the addon,
  keeps only the code the addon reaches and exports only the two Node-API
  entry points, so the addon is a single file that needs only the system's
  libraries: `make -C javascript build CONFLUENT_KAFKA_LINK=static`.

## Packaging

The release is three npm packages: the main package (`dist/`, this README,
the changelog, the LICENSE and the NOTICE, which carries Apache Kafka's credit
line) and one platform package per platform, built from its folder of `npm/`,
each holding the statically linked addon, the LICENSE, the NOTICE and
`THIRD_PARTY_LICENSES.txt`, the licences of the Rust crates linked into the
addon, which `.semaphore/third-party-licenses.sh` builds with cargo-about
(`npm/about.toml`). The main package's `package.json` is the repository's
without `private`, `devDependencies` and `scripts`, with the two platform
packages as `optionalDependencies` at its exact version.

```bash
make -C javascript pack        # this machine's static addon, packed into build/pack/ with the
                               # main package (.semaphore/pack-npm.sh --dev), and checked; it
                               # builds the third-party licence file, installing cargo-about
                               # into rust/target/ when it is not on PATH
make -C javascript pack-check  # then installed into a fresh project from the tarballs,
                               # where the smoke program runs (.semaphore/test-npm.sh)
```

`make package-check-javascript`, from the repository root, builds the Rust
library first; the macOS CI job runs it on every build. The release packages
are built, tested and recorded by the tag build of `javascript/v<version>`
(the "Release (JavaScript):" blocks of `.semaphore/semaphore.yml`), and
published from Semaphore by that build's "Publish to npm" promotion
(`.semaphore/publish-npm.yml`), with a granular npm token and no provenance:
a pre-release under the `beta` dist-tag only, which a check refuses to put
on `latest`. `tools/package-smoke/` is the smoke program the install tests
run; it is not published. `cargo xtask bump-version --javascript-version <v>`
(`make bump-version JAVASCRIPT_VERSION=<v>`) sets the version of
`package.json`, its lockfile and every platform package of `npm/` together; its
`MAJOR.MINOR` is the Apache Kafka release in `kafka/`, and its `PATCH` is
`AK_patch × 100 + core_patch × 10 + binding_patch` (`rfc/versioning.md`).

### The Rust multilanguage suite

The Rust integration tests' `multilanguage_test!` producer cases also run
against this client, as their `__grpc_js` arm, through
`grpc-server/grpc_server.js` (a gRPC server on port 50054 over
`rust/multilanguage-test-server/proto`'s `ProducerService`, and the chaos
harness's `ChaosWorkloadService`; it is not part of the published package):

```bash
make test-integration-javascript-native  # from the repository root: the server as a host process
make test-integration-javascript         # Linux only: the server in a container (Dockerfile.grpc)
```

The native arm loads `@grpc/grpc-js` and `@grpc/proto-loader`, two of the
devDependencies, from `javascript/node_modules` (`npm ci`);
`make -C javascript grpc-native` checks they are there. The transaction and
metrics cases are skipped for this arm (`JS_GRPC_SKIPS` in `rust/Makefile`),
as the producer has none of them yet.

## Layout

Each Java package has one import path, and each imports from its own path
only: no index re-exports another package. The package root holds only Java's
built-in errors (`IllegalStateError`, `ExecutionError`, ...) and the
`Duration` type:

```js
const { ExecutionError, IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { KafkaProducer, ProducerRecord, RecordMetadata } = require('@confluentinc/kafka-javascript/producer');
const { KafkaError, KafkaFuture } = require('@confluentinc/kafka-javascript/common');
const { SaslAuthenticationError } = require('@confluentinc/kafka-javascript/common/errors');
const { SslConfigs, SaslConfigs } = require('@confluentinc/kafka-javascript/common/config');
```

The supported import forms are `require` (above), named imports and a
namespace import, from an ES module or from TypeScript, and an ES module's
default import under Node's own loader (`.mjs`, `.mts`, `"type": "module"`):

```js
import { ExecutionError } from '@confluentinc/kafka-javascript';
import { KafkaProducer } from '@confluentinc/kafka-javascript/producer';
import * as errors from '@confluentinc/kafka-javascript/common/errors';
```

A deep import (`@confluentinc/kafka-javascript/dist/producer/kafka_producer`,
`.../producer/kafka_producer`) fails with Node's `ERR_PACKAGE_PATH_NOT_EXPORTED`:
import each package from its path. For a TypeScript setup or a tool that does
not read `exports`, the published package also has the folders behind the
import paths (`producer/`, `common/config/`, ...), each with a `package.json`
pointing into `dist/`.

A default import compiled to CommonJS by an interop helper (TypeScript's
`esModuleInterop`, Babel, a bundler's interop) does not work: the compiled
package sets `__esModule`, so the helper reads a `default` export it does not
have. TypeScript rejects such an import (TS1192, "has no default export");
import by name or as a namespace instead.

A TypeScript project needs `@types/node` (`npm install --save-dev
@types/node`): the declarations type keys and values with Node's `Buffer`, and
the package does not depend on `@types/node` itself, so without it (and without
`skipLibCheck`) tsc reports TS2688 / TS2580 in `producer_record.d.ts`.

```
src/index.ts           the Java built-in errors (IllegalStateError, IllegalArgumentError, ...)
                       and the Duration type
src/producer/          KafkaProducer, ProducerRecord, RecordMetadata, Callback (a type),
                       ProducerConfig, BufferExhaustedError
src/common/            KafkaError, KafkaFuture; errors/ (Java's common.errors), config/
                       (SslConfigs, SaslConfigs, SecurityConfig, SslClientAuth, ConfigError),
                       security/auth/ (SecurityProtocol), ...
src/consumer/          the consumer's errors only (no consumer yet)
src/internal/          private helpers (not exported, not declared)
dist/                  src/ compiled by tsc (tsconfig.json): .js, .d.ts and maps, the
                       package's entry points (git-ignored)
native/                the addon: util.h, addon.c, callback_queue.c, error.c, record.c, producer.c,
                       and binding.gyp (node-gyp runs here, never at install)
test/static|unit|typing|integration
npm/                   one folder per platform package (its package.json and README)
```

The addon builds as `native/build/Release/confluent-kafka-javascript.node`,
beside `dist/`. `src/internal/native_loader.ts` loads the platform package's addon
first and falls back to that development build; installing the package runs no build
(`"gypfile": false`, no install scripts).

## Example

```js
const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');

const producer = new KafkaProducer({ 'bootstrap.servers': 'localhost:9092' });  // or { configs: {...} }
const future = await producer.send({                   // resolves at hand-over
  record: new ProducerRecord({ topic: 'demo', key: 'k1', value: 'hello' }),
  // Optional, Java's Callback.onCompletion(metadata, exception): runs before the future completes.
  callback: ({ metadata, exception }) => { /* ... */ },
});
const metadata = await future.get();                   // resolves at delivery
console.log(metadata.topic, metadata.partition, metadata.offset, metadata.timestamp);
await producer.flush();
await producer.close();          // or close(5000) / close({ timeout: 5000 }), milliseconds or a Temporal.Duration
```

`await using producer = new KafkaProducer(...)` closes the producer when its
scope ends (`[Symbol.asyncDispose]` calls `close()`), where the runtime
supports `await using` (Node 24 and later). When the block throws and
`close()` throws too, JavaScript raises a `SuppressedError`, whose `error` is
`close()`'s error and whose `suppressed` is the block's.

### The callback

The callback is Java's `Callback.onCompletion(metadata, exception)` as a
function of one object, named as Java names its parameters:
`({ metadata, exception }) => ...`. `exception` is `null` on success, else the
delivery error, and `metadata` then has Java's `-1` fields. It runs on the JS
thread, before the record's future completes. A callback that returns a
Promise is awaited: the record's future completes, and a `flush()` or
`close()` that covers the record returns, only once that Promise settled, as
Java's wait for the callback to return. A rejection is handled as a throw:
the client logs it, and it reaches no caller. Do not wait inside the
callback's Promise for `flush()` or `close()` of the same producer: they wait
for it.

Inside a callback, do not call `flush()` or `close()` of the same producer,
do not run a transaction call, and do not await the future of a record sent
later: each waits for the callback itself. Before the callback's first
`await` the producer refuses them; after it they can hang. A callback
Promise that never settles has no time limit, as in Java: it holds its
record's future, `flush()` and `close()`, `close({ timeout })` included.

### Parameters by name

Every method and constructor takes its parameters as one object, named as
Java names them, and the names given must be one of Java's overloads:
`new KafkaProducer({ configs })` (Java's `Map` and `Properties` constructors;
`configs` is a plain object, and a `Map` throws `TypeError`),
`send({ record })` (Java's `send(record)`, which passes a `null` callback) /
`send({ record, callback })`, `close()` (Java's, which waits as
`close(Duration.ofMillis(Long.MAX_VALUE))` does) / `close({ timeout })`, and `ProducerRecord`'s six constructors, from
`{ topic, value }`, `{ topic, key, value }` and `{ topic, partition, key, value }`
to `{ topic, partition, timestamp, key, value, headers }`. So a record with
headers or a timestamp names its partition, `null` to let the producer choose.
Anything else throws (or rejects with) an `IllegalArgumentError` listing the
overloads:

```
ProducerRecord() takes one of (topic, partition, timestamp, key, value, headers), ..., (topic, value); got (topic, value, headers)
```

An overload with one parameter also takes the value on its own:
`send(record)`, `close(5000)`, `SecurityProtocol.forName('ssl')`, and
`new KafkaProducer({ 'bootstrap.servers': ... })`, the configs themselves. A
plain object is read as `{ configs }` only when it has a `configs` key, so a
config key next to `configs` is refused as an overload no Java constructor has.
A bare value picks its overload by its type, and one that fits none throws
(or rejects with) an `IllegalArgumentError` in the same form:
`send() takes one of (record), (record, callback); got a number`. A method
that only reads (`isDone()`, `name()`, `key()`, `toArray()`) throws
`TypeError` when it is given any argument.

Java's `send(record, callback)` is `send({ record, callback })`: a second
argument is refused with an `IllegalArgumentError`, never dropped.

`null` is Java's `null`. To leave a parameter out, leave its key out, or the
argument: a key set to `undefined`, a bare `undefined` (`forName(undefined)`,
`close(undefined)`, `flush(undefined)`) and `undefined` inside the headers
throw `TypeError` (`"value must not be undefined. Pass null for Java's null,
which sends a tombstone."`), except a config value, which is dropped. A
`timeout` is a `Duration` (exported from the package root): a number of
milliseconds or a `Temporal.Duration` (built into Node 26, from a polyfill
such as `@js-temporal/polyfill` before), rounded down to whole milliseconds.
`Infinity` means no limit; `NaN`, `-Infinity`, a number above
`Number.MAX_SAFE_INTEGER` and a `Temporal.Duration` with years, months or
weeks throw `RangeError`, a negative one Java's `IllegalArgumentError` (from
the client's core, which closes nothing then), and `null` `TypeError('timeout
must not be null')`. A `close()` on a producer already closing or closed is a
close of its own, in the client, and checks its timeout the same way. The
methods returning a Promise (`send`, `flush`, `close`) never throw: every
failure rejects their Promise.

### Headers

`Header` and `Headers` are in `common/header`, with Java's members. A
`Header` keeps Java's methods: `new Header({ key, value })`, then
`header.key()` and `header.value()`. The value is bytes or `null`, as Java's
`byte[]`: it is taken as a `Uint8Array` (a `Buffer` is one) and returned as a
`Buffer` over the same bytes, so a string value throws `TypeError`: encode it
first. `Headers` keeps Java's methods: `add(header)` or `add({ key, value })`,
`remove(key)`, `lastHeader(key)`, `headers(key)`, `toArray()`, iteration
(`for (const header of headers)`) and Java's `toString()`; `new Headers()`,
`new Headers(headers)` or `new Headers({ headers })`.

Where headers are taken (`ProducerRecord`'s `headers`, `new Headers(...)`),
they are a `Headers` or an iterable of `Header`s, as Java takes them:

```js
const { Header, Headers } = require('@confluentinc/kafka-javascript/common/header');
const record = new ProducerRecord({
  topic: 'demo', partition: null, key: 'k', value: 'v',
  headers: [new Header({ key: 'trace', value: Buffer.from('abc') }), new Header({ key: 'flag', value: null })],
});
record.headers.add({ key: 'more', value: Buffer.from([1]) });
```

A record's `headers` is its own `Headers` (an empty one when it has none),
copied from what was given, as Java's `new RecordHeaders(headers)`. `send()`
makes it read-only once the record's fields are checked: a later `add` or
`remove` throws `IllegalStateError` "RecordHeaders has been closed.", as in
Java. They are sealed before the core's metadata wait *(deviation: Java's
`send()` seals them only after that wait, so in Java a record whose send times
out on metadata keeps writable headers. Why: the headers cross the C ABI
during `send()`, before the core waits for metadata, and the binding cannot see
when that wait ends. Goes when: the core seals the headers itself, after its
metadata wait)*.

### Data objects, enums and constructors

`ProducerRecord`, `RecordMetadata`, `TopicPartition` and `Node` are data objects: Java's getters are
public `readonly` fields, set once in the constructor (`record.topic`,
`metadata.offset`, `metadata.hasOffset`), so `{ ...metadata }`,
`JSON.stringify` and `structuredClone` keep them. Java's `-1` and `null` are
kept as Java returns them (`offset` is `-1n` when `hasOffset` is `false`).
A Java `long` is a `bigint`: `metadata.offset`, `metadata.timestamp` and
`record.timestamp` read as bigints, exact at every value, and a timestamp
given to `ProducerRecord` is a bigint or a whole number up to 2^53 - 1 (a
larger number throws `RangeError` rather than rounding). `JSON.stringify`
writes a bigint as a decimal string, such as `"42"`, which `BigInt()` reads
back exactly. A Java `int`, such as a partition, stays a number, and one
outside Java's range throws `RangeError`. A data object is frozen: a
field cannot be reassigned (in strict mode the assignment throws
`TypeError`), so `send()` sends what its constructor checked. A record's
`headers` is
a `Headers`, a class with methods: `JSON.stringify` writes it as its array of
headers, and `structuredClone` as an empty object. Every other
class keeps Java's getters as methods (`future.isDone()`, `header.key()`,
`partitionInfo.replicas()`). A Java array or collection a method takes is any
iterable (`new PartitionInfo({ ..., replicas: new Set(nodes) })`), and one it
returns is a new array each call. `TopicPartition`, `Node` and
`PartitionInfo` have Java's `equals` and `hashCode`.

The enums, `SecurityProtocol` and `SslClientAuth`, have Java's `Enum`
methods: `name()`, `ordinal()`, static `values()` and static
`valueOf(name)` (exact, else `IllegalArgumentError` "No enum constant
org.apache.kafka.common.config.SslClientAuth.none"); `JSON.stringify`
writes the name, and so does `toString()`, unless Java overrides it
(`SslClientAuth.REQUIRED.toString()` is `'required'`, as Java's). Java's public
final fields stay fields (`SecurityProtocol.SSL.id`). A constant Java `List`
is a frozen array, and a constant `Set` (`SslConfigs.RECONFIGURABLE_CONFIGS`)
a read-only set whose `add`, `delete` and `clear` throw
`UnsupportedOperationError`.

`KafkaFuture` and the enums are built by the client only: their constructors
are private. `RecordMetadata` has Java's public constructor, `new
RecordMetadata({ topicPartition, baseOffset, batchIndex, timestamp,
serializedKeySize, serializedValueSize })`.

### `send` in two steps

`send({ record, callback })` returns a `Promise<KafkaFuture<RecordMetadata>>`,
the shape of Java's `send()`, which blocks until the record is in the
producer's buffer and then returns a `Future`:

1. The `send()` Promise resolves once the record has been **handed over** to
   the producer: at once, or after waiting up to `max.block.ms` for metadata
   or buffer memory. It rejects with the error Java's `send()` would throw (a
   closed producer, a bootstrap resolution failure, ...); such an error never
   reaches the callback, so do not leave a `send()` Promise unhandled.
2. The future's `get()` resolves at **delivery** with the record's
   `RecordMetadata`, or rejects with an `ExecutionError` (Java's
   `ExecutionException`) whose `cause` is the delivery error. The callback,
   when given, runs first, on the JS thread, and receives the delivery error
   itself: from the event loop, or inside a later `send()` call, which runs
   the callbacks already due before handing its record over. A future whose
   `get()` is never called raises no unhandled rejection, and neither do the
   futures that depend on it; a `get()` Promise that is dropped does, as any
   rejected Promise. `isDone()` and `isCompletedExceptionally()` tell its
   state; `cancel()` returns `false` (Java's `FutureRecordMetadata`).

`KafkaFuture` has Java's other members too, each taking its parameter on its
own or by name: `get(timeout)` (a `Duration`; the root package's
`TimeoutError`, Java's `java.util.concurrent.TimeoutException`, when it elapses),
`getNow(valueIfAbsent)`, `thenApply(fn)` and `whenComplete(action)` (called
as `fn(value)` and `action(value, error)`, the raw error), `toCompletionStage()`
(a `Promise` settling with the future, rejecting with the raw error), and the
static `KafkaFuture.completedFuture(value)` and `KafkaFuture.allOf(futures)`.
A plain object is always the options object, so it is passed by name,
`KafkaFuture.completedFuture({ value: obj })` or
`future.getNow({ valueIfAbsent: obj })` (generic code should always do so);
any other value may be given on its own. In TypeScript that is a primitive,
`null`, an array, a function, a `Map`, `Set`, `Date`, `ArrayBuffer`,
`DataView` or typed array (`Buffer` included), the values no plain object can
be; an instance of another class goes by name there, while plain JavaScript
also takes it on its own.
The futures these make can be cancelled, `cancel(true)`, which completes them,
and their own dependants, with a `CancellationError` (Java's
`CancellationException`), never the record's future they come from.

**Ordering is the caller's.** Records from code that awaits each `send()`
before the next reach the producer in that order (records to one partition
are appended in send order) and are covered by a following `flush()` or
`close()`: when `flush()` resolves, every such record's future is done.
Sends that are not awaited one by one (e.g. `Promise.all(records.map((record) =>
producer.send({ record })))`) are concurrent, like Java `send()` calls on several
threads: there is no order between them, and a `flush()` or `close()`
started before they resolved does not wait for them.

**Keys and values are not copied.** The client reads a `Buffer` or
`Uint8Array` key or value after `send()` returns. Don't write to it, transfer
it or resize it until the Promise from `send()` settles; awaiting `send()` is
enough. A serializer must return a new Buffer for each record. A `Buffer`
over a `SharedArrayBuffer` is borrowed the same way: if other workers share
the Buffer's memory, they mustn't write to it until then either. Header
values are copied during `send()`.

**Strings are encoded as Java encodes them.** A string key or value is
encoded to UTF-8 as Java's `StringSerializer` does (`String.getBytes(UTF_8)`):
a lone surrogate, which UTF-8 cannot hold, becomes `?`, where Node's own
`Buffer.from` writes U+FFFD. Header keys and config keys and values are
encoded the same way; topic names are not.

**Close every producer.** A producer keeps Node running from the moment it's
created until `close()` closes it, as KafkaJS and 1.x do from `connect()` to
`disconnect()`, so no pending send is cut off by Node exiting in the middle
of an `await`. A script that never calls `close()` never exits on its own,
even an idle one, and no garbage collection closes a producer, even once
nothing refers to it *(deviation: Java's client threads are daemon threads,
so a program can exit with a client still open)*. Once closed, a producer no
longer holds Node, and its native client is released when the producer is
garbage collected. A call made after `close()` goes to the client, which
answers as Java's does: `send()` rejects with `IllegalStateError` "Cannot
perform operation after producer has been closed", and `flush()` and a second
`close()` resolve. A call made after `close()` has completed settles only
while something else keeps the event loop running: a closed producer no
longer holds Node, so a script whose only pending work is such a call can
exit before it settles. A `process.exit()` runs no
cleanup: the process ends at once and the unsent records are lost, as Java's
daemon I/O thread loses unsent records at JVM exit. The preview doesn't
support clients in worker threads. Create clients in the main thread, and
send plain data to and from workers (Limitations, below).

`examples/producer.js` produces one record, awaiting its hand-over and then
its delivery, and prints its metadata:

```bash
docker run -d --rm --name kafka-js -p 9092:9092 apache/kafka:4.3.1
docker exec kafka-js /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic js-demo --partitions 1 --replication-factor 1
node javascript/examples/producer.js localhost:9092 js-demo k1 hello-from-node
# RecordMetadata topic=js-demo partition=0 offset=0 timestamp=...
```

Any consumer reads it back, e.g. the Python verifiable consumer (after
`make build-python`):

```bash
venv/bin/python python/verifiable_clients/verifiable_consumer.py --bootstrap-server localhost:9092 \
  --topic js-demo --group-id js-verify --group-protocol consumer --max-messages 1 --verbose \
  --reset-policy earliest
```

## Migrating from the earlier development builds

(For 1.x and KafkaJS, see [`MIGRATION.md`](MIGRATION.md).)

The development builds before `4.4.0-beta.1` had another call shape. Every
change below is user-visible. Most turn a call that ran into one that throws
or rejects, so a test run finds them; the rows marked **silent** do not, and
code relying on them keeps running with another meaning: check each.

| Before | Now |
|---|---|
| `send(record, callback)` | `send({ record, callback })`, or `send(record)` without a callback; a second argument is refused with `IllegalArgumentError` |
| a callback `(metadata, error) => ...` | **silent**: `({ metadata, exception }) => ...`, Java's parameter names; an unchanged callback gets the one object as `metadata` and `undefined` as `error`, so its failure branch never runs. It may return a Promise, which is awaited |
| `close({ timeoutMs })` | `close(timeout)` or `close({ timeout })`, a `Duration`: milliseconds or a `Temporal.Duration`, `Infinity` for no limit |
| `new KafkaProducer(configs)` | the same, or `new KafkaProducer({ configs })` |
| `new ProducerRecord({ ... })` with any subset of the fields | exactly one of Java's six constructors: a record with headers or a timestamp names `partition` (`null` lets the producer choose), and `{ topic, partition, value }` is no Java constructor |
| a field set to `undefined` meant "not given" | leave the key out; `undefined` throws `TypeError` (`null` is Java's null), except a config value, which is still dropped |
| headers as `{ key, value }` objects or `[key, value]` pairs, string values accepted | `Header` objects, the value bytes or `null`; a string value throws `TypeError` |
| `header.key`, `header.value` | `header.key()`, `header.value()`, Java's methods; the value is a `Buffer` |
| `require('@confluentinc/kafka-javascript').producer`, `.common.errors` (namespaces) | gone: import each package from its own path, `@confluentinc/kafka-javascript/producer`, `.../common/errors`; the root keeps the Java built-in errors and `Duration` |
| `CloseOptions` (a type) | `CloseArgs`, the options of `close(Duration)`, `{ timeout }` |
| `close(undefined)`, `flush(undefined)`, `get(undefined)` were the calls without an argument | they throw (or reject with) `TypeError`: leave the argument out |
| a bare value of the wrong type threw `TypeError` | it fits no overload: `IllegalArgumentError` "… takes one of (…); got a number" |
| a Temporal.Duration with years, months or weeks was an `IllegalArgumentError` | **silent** for `instanceof RangeError`: it is a plain `RangeError` |
| `SslConfigs.RECONFIGURABLE_CONFIGS` a frozen array (`.includes`) | a read-only set (`.has`) |
| `key.serializer` / `value.serializer` reached the core | they throw `ConfigError`: a serializer is a constructor argument |
| `metadata.offset` / `metadata.timestamp` / `record.timestamp` numbers | bigints, Java's `long`; a timestamp is given as a bigint or a whole number up to 2^53 - 1 |
| a config value was checked against its key's type, and an unknown key's value had to be a string, a number or a boolean | every value is written as a string (`true` as `"true"`, numbers and bigints in decimal, arrays joined with `,`, anything else by `String()`) and the core checks it; `null` and `undefined` leave the key unset |
| the preview warning had the code `CONFLUENT_KAFKA_PREVIEW` | it has no code: filter on its type, `ExperimentalWarning` |
| `record.headers` an array | `record.headers` a `Headers` (Java's methods: `add`, `remove`, `lastHeader`, `headers(key)`, `toArray()`, iteration), made read-only by `send()` |
| `ProducerRecord` / `RecordMetadata` getters | the same names, now own `readonly` fields, which a spread and `JSON.stringify` keep; `structuredClone` / `postMessage` keep every field but `headers`, which becomes `{}` |
| `SecurityProtocol.SSL.name`, `SslClientAuth.REQUIRED.name` | `SecurityProtocol.SSL.name()`, `SslClientAuth.REQUIRED.name()`; the enums also have `ordinal()`, `values()`, `valueOf()` and `toJSON()` |
| `SecurityProtocol.forId(null)` returned `null` | it throws `TypeError` (an unknown number still returns `null`) |
| a failed future's `get()` rejected with the delivery error | **silent** for `instanceof`: it rejects with `ExecutionError`, whose `cause` is the delivery error (the callback still gets the error itself) |
| `err.code`, `err.errorName`, `err.retriable`, `err.apiError` | **silent**: gone, they read `undefined`; classify with `instanceof` (`RetriableError`, `ApiError`, ...), `err.name` as the fallback |
| `new KafkaError(message, { cause })` | `new KafkaError({ message, cause })`, `new KafkaError(message)` or `new KafkaError(cause)`; Java's abstract bases throw `TypeError` when constructed |
| `new QuotaViolationError(message)` and the other errors whose every Java constructor takes a payload | they throw `TypeError`: `QuotaViolationError`, `RecordDeserializationError`, `CorrelationIdMismatchError`, `LogTruncationError`, `NoOffsetForPartitionError` and the consumer's `OffsetOutOfRangeError` |
| `IllegalArgumentError extends Error` | it, and `IndexOutOfBoundsError`, extend `RangeError` |
| `send()` threw on a closed producer or a bad argument | every method returning a Promise rejects instead, and never throws |
| an argument to `flush()` | it is refused: Java's `flush()` takes none |
| a lone surrogate in a string was written as U+FFFD | it is written as `?`, as Java's `StringSerializer` writes it |
| an idle producer let the process exit | a producer keeps Node running until `close()` closes it, and no garbage collection closes one: close every producer (or `await using` it) |
| `KafkaFuture` had `get()`, `isDone()`, `isCompletedExceptionally()`, `isCancelled()`, `cancel()` | Java's whole `KafkaFuture`: also `get(timeout)`, `getNow`, `thenApply`, `whenComplete`, `toCompletionStage()`, `completedFuture` and `allOf`; `cancel(mayInterruptIfRunning)` takes a boolean |
| the enums guarded their constructors with a run-time token (`RecordMetadata`'s was TypeScript `private` only) | their constructors are TypeScript `private`, with no run-time guard |
| `KafkaFuture` guarded its constructor with a run-time token | it is an abstract class, as Java's is: `new KafkaFuture()` throws `TypeError`, and a subclass may be built |
| Node.js 18 | Node.js 22.14 or newer |

The entry points (`@confluentinc/kafka-javascript`, `/producer`, `/common`,
...) keep their paths, but no index re-exports another package any more. `ProducerRecordOptions` is no longer exported: each Java
constructor is a signature of its own. The `Header` type is no longer
exported from `/producer`: `Header` is now a class, in `/common/header`
(`import { Header } from '@confluentinc/kafka-javascript/common/header'`). `Duration` is exported from the
package root.

## Limitations

- **One copy of the package per application.** The client's classes are
  checked by `instanceof`: a `ProducerRecord`, `Header` or `Headers` made by
  one copy of the package is refused by a `KafkaProducer` of another copy
  (`send(record)` throws `IllegalArgumentError` "send() takes one of (record),
  (record, callback); got a ProducerRecord", and `send({ record })` throws
  `TypeError` "record must be a ProducerRecord"), and an error of one copy is
  no `instanceof` the other copy's classes. Make sure npm installs the
  package once (`npm ls @confluentinc/kafka-javascript`; deduplicate with
  `npm dedupe` or an `overrides` entry).
- **No clients in worker threads.** The preview doesn't support clients in
  worker threads. Create clients in the main thread, and send plain data to
  and from workers. A worker that ends with its client still open, through
  `worker.terminate()`, `process.exit()` or an uncaught error, may crash the
  whole process. Nothing refuses a client made in a worker, and a
  client's objects (a producer, a future, a record) stay in the thread that
  made them. A record's fields cross as they are once copied out, except its
  headers: `postMessage` and `structuredClone` copy a `Headers` as an empty
  object, so convert them to plain pairs first, and rebuild the record and
  its `Header`s on the other side:

  ```js
  // In a worker: plain data (Buffers and Uint8Arrays cross as Uint8Arrays).
  const message = {
    topic: record.topic, partition: record.partition, timestamp: record.timestamp,
    key: record.key, value: record.value,
    headers: record.headers.toArray().map((h) => [h.key(), h.value()]),
  };
  parentPort.postMessage(message);
  // In the main thread, which owns the producer: a record of its own.
  const copy = new ProducerRecord({
    ...message,
    headers: message.headers.map(([key, value]) => new Header({ key, value })),
  });
  ```

  A delivered record's `RecordMetadata` is a data object: `{ ...metadata }`
  is plain data too.

## Security

`security.protocol` takes `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL`
(`SecurityProtocol`). The SSL keys are Java's (`SslConfigs`); stores are PEM,
as files (`ssl.truststore.location`, and for mTLS `ssl.keystore.location`, one
file holding the client certificate chain and its private key) or inline
(`ssl.truststore.certificates`, `ssl.keystore.certificate.chain` with
`ssl.keystore.key`). SASL credentials go in `sasl.jaas.config`, the only way to
give them:

```js
const producer = new KafkaProducer({
  configs: {
    'bootstrap.servers': 'broker:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.jaas.config':
      'org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="secret";',
    'ssl.truststore.location': '/etc/kafka/ca.pem',
  },
});
```

Limitations: of the SASL mechanisms, only `PLAIN` is supported (any other,
Java's `GSSAPI` default included, fails the configuration with a
`ConfigError`), so `sasl.mechanism` must be set under `SASL_*`. Keys and
certificates must be PEM (no JKS or PKCS12), and encrypted private keys are
rejected. `SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE` / `DEFAULT_SSL_TRUSTSTORE_TYPE`
are Java's `JKS`; the client's own default is `PEM`. A password-type value of
the wrong JavaScript type is reported without its value.

## Errors

The error classes are Java's, `Exception` read as `Error`, with Java's
`extends` chain: `SaslAuthenticationError extends AuthenticationError extends
ApiError extends KafkaError extends Error`, `NotLeaderOrFollowerError` is a
`RetriableError`, and so on (`common/errors/`, generated from the Java
sources). Every error the client reports is an instance of the class of its
Java exception, so `instanceof` tells them apart, `name` (the class name) being
the fallback; an error has no other public field than `name`, `message` (Java's
`getMessage()`) and `cause` (Java's `getCause()`, `Error`'s `cause`). Use
`instanceof` (or `name`), not `err.code` or `err.retriable`: a retriable error
is an `instanceof RetriableError`, a broker error an `instanceof ApiError`.

```js
const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { RetriableError } = require('@confluentinc/kafka-javascript/common/errors');

try {
  await (await producer.send(record)).get();
} catch (err) {
  // send() rejects with the error itself; get() with an ExecutionError, the
  // delivery error being its cause, as Java's Future.get() throws.
  const e = err instanceof ExecutionError ? err.cause : err;
  if (e instanceof RetriableError) { /* retry */ } else throw err;
}
```

There are two `TimeoutError` classes: the root package's, Java's
`java.util.concurrent.TimeoutException`, which a timed `future.get(timeout)`
rejects with when the wait elapses, and `common/errors`' Kafka
`TimeoutError`, which a delivery or `max.block.ms` timeout fails a record
with (the `cause` of `get()`'s `ExecutionError`, and the callback's
`exception`). Neither is the `DOMException` that `AbortSignal.timeout()`
throws.

A Rust panic that the core catches reaches the caller unchanged, as the
error of the call it happened in: close that client and create a new one.

An error the client has nowhere else to report (a failed report to the core,
an error freeing a native object) goes to `process.emitWarning`.

Java's built-in exceptions (`IllegalStateError`, `IllegalArgumentError`, ...)
are at the package root and, as in Java, are not `KafkaError`s: `send()` on
a closed producer rejects with an `IllegalStateError`.
`IllegalArgumentError` and `IndexOutOfBoundsError` extend JavaScript's
`RangeError`, `CancellationError` extends `IllegalStateError`, as Java's
`CancellationException` extends `IllegalStateException`, and the others
`Error`.

The constructors are Java's forms with a message and a cause, by name or, for
a form with one parameter, the value on its own: `new KafkaError('boom')`,
`new KafkaError({ message: 'boom', cause })`, `new KafkaError(cause)` (whose
message is the cause's `String()`, as Java's is `cause.toString()`) and
`new KafkaError()`. A second argument (the ES2022 `new Error(message,
{ cause })`) throws an `IllegalArgumentError` pointing to
`new KafkaError({ message, cause })`. A class takes only its own Java forms
(`TopicAuthorizationError` has no `(message, cause)` one, so
`new TopicAuthorizationError({ message, cause })` throws
`IllegalArgumentError`); Java's abstract bases (`RetriableError`, ...) throw
`TypeError` when constructed. The constructors that take a payload
(`TopicAuthorizationError`'s topics, ...) are not translated yet, and a class
whose every Java constructor takes one (`QuotaViolationError`, ...) cannot be
constructed: the client builds it when the core reports it. The
configuration is checked by the client's core, which throws a `ConfigError`
with Java's message naming the key (`{ 'linger.ms': true }` fails with "Not a
number of type LONG"); `key.serializer` and `value.serializer` throw a
`ConfigError` too (a serializer is a constructor argument). Any other value
of the wrong JavaScript type throws a `TypeError`, and so does a topic, header key or config key or value holding a
null byte (`"topic must be a string without null bytes"`), which C would read
as the end of the string; keys, values and header values are bytes and may
hold any. The core's error for a null mandatory argument (`LOCAL_NULL_POINTER`,
Java's `NullPointerException`) is a `TypeError` as well, as in the Python
client; the client checks its arguments first, so it is not expected.

See `CLAUDE.md` for the binding's rules.
