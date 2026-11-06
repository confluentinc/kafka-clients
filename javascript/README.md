# Confluent Kafka JavaScript (Node.js) client — preview

A Node.js client for Apache Kafka built on the C FFI of the Confluent Kafka
Rust client, in the same way as the Python client in `../python`. It is a
Node-API addon written in plain C (`src/`), so one build works on every Node
release supporting Node-API version 8, under a thin layer written in
TypeScript (`lib/`), compiled into CommonJS with its type declarations
(`dist/`), so it is used from CommonJS (`require`), from ES modules (`import`)
and from TypeScript alike (see "Layout" for the import forms).

The API follows the Java client's shape (`KafkaProducer`, `ProducerRecord`,
`RecordMetadata`, `KafkaFuture`), with JavaScript idioms (a `Promise` for a
blocking call, a `KafkaFuture` whose `get()` returns a `Promise`). This first
slice covers the producer only.

The package is `@confluentinc/kafka-javascript`, the npm name of the
librdkafka-based client, at version `4.4.0-beta.1`: like the Rust-based Python
client, which reuses PyPI's `confluent-kafka` at `4.4.0b1`, it is told apart
from the earlier client (0.x and 1.x releases) by version, 4.0.0 or higher.
Its pre-releases are published under npm's `preview` dist-tag, never `latest`,
`rc` or `alpha`, which the earlier client's 1.x releases hold. The
repository's `package.json` stays `private`; the release pipeline writes the
published one (see "Packaging").

## Install from npm

```bash
npm install @confluentinc/kafka-javascript@preview
```

Prebuilt for Linux x64 and arm64 with glibc 2.28 or newer (RHEL 8, Debian 10,
Ubuntu 20.04 and later) and for macOS 11 or newer on Apple silicon, on
Node.js 18 or newer. The main package carries the compiled library only; npm
installs the one platform package of the machine,
`@confluentinc/kafka-javascript-linux-x64-gnu`, `-linux-arm64-gnu` or
`-darwin-arm64`, from its optional dependencies. Each carries the addon with
the Rust client linked in, so nothing is built at install time and
`--ignore-scripts` changes nothing; do not install with `--omit=optional`.
Installing for another machine (a container image built on a laptop) needs
npm's `--os`, `--cpu` and `--libc` of that machine. musl (Alpine) and Windows
have no platform package yet: build from source there.

## Requirements

- Node.js 18 or newer, with its headers (official tarballs, Homebrew and Nix
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
make -C javascript build             # the addon (node-gyp) and the library (tsc, lib/ -> dist/);
                                     # or, from the repository root: make build-javascript
make -C javascript test-static       # header conformance and package layout, no build needed
make -C javascript test-unit         # no broker needed
make -C javascript test-typing       # dist/'s declarations type-check under --strict
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

`CONFLUENT_KAFKA_LINK` (binding.gyp) chooses how the addon links the library:

- `dynamic`, the default, for development, the tests and the gRPC image: it
  links `libconfluent_kafka.{so,dylib}` with an rpath to that directory.
- `static`, for a release: it links `libconfluent_kafka.a` into the addon,
  keeps only the code the addon reaches and exports only the two Node-API
  entry points, so the addon is a single file that needs only the system's
  libraries: `make -C javascript build CONFLUENT_KAFKA_LINK=static`.

## Packaging

The release is four npm packages: the main package (`dist/`, this README, the
changelog and the LICENSE) and one platform package per platform, each holding
the statically linked addon. The main package's `package.json` is the
repository's without `private`, `devDependencies` and `scripts`, with the
three platform packages as `optionalDependencies` at its exact version.

```bash
make -C javascript pack        # this machine's static addon, packed into build/pack/ with the
                               # main package (.semaphore/pack-npm.sh --dev), and checked
make -C javascript pack-check  # then installed into a fresh project from the tarballs,
                               # where the smoke program runs (.semaphore/test-npm.sh)
```

`make package-check-javascript`, from the repository root, builds the Rust
library first; the macOS CI job runs it on every build. The release packages
are built, tested and recorded by the tag build of `javascript/v<version>`
(the "Release (JavaScript):" blocks of `.semaphore/semaphore.yml`), and
published to npmjs.com by hand from the recorded bundle
(`release/README.txt`). `tools/package-smoke/` is the smoke program the
install tests run; it is not published.

### The Rust multilanguage suite

The Rust integration tests' `multilanguage_test!` producer cases also run
against this client, as their `__grpc_js` arm, through `grpc_server.js` (a gRPC
server over `rust/multilanguage-test-server/proto`'s `ProducerService`; it is
not part of the published package):

```bash
make test-integration-javascript-native  # from the repository root: the server as a host process
make test-integration-javascript         # Linux only: the server in a container (Dockerfile.grpc)
```

The native arm loads `@grpc/grpc-js` and `@grpc/proto-loader`, two of the
devDependencies, from `javascript/node_modules` (`npm ci`);
`make -C javascript grpc-native` checks they are there. The transaction, `partitionsFor` and
metrics cases are skipped for this arm (`JS_GRPC_SKIPS` in `rust/Makefile`),
as the producer has none of them yet.

## Layout

The package root exports the module namespaces, one per Java package, and each
namespace is also an entry point of its own:

```js
const { producer, common } = require('@confluentinc/kafka-javascript');
const { KafkaProducer, ProducerRecord, RecordMetadata } = require('@confluentinc/kafka-javascript/producer');
const { KafkaError, KafkaFuture } = require('@confluentinc/kafka-javascript/common');
const { SaslAuthenticationError } = require('@confluentinc/kafka-javascript/common/errors');
const { SslConfigs, SaslConfigs } = require('@confluentinc/kafka-javascript/common/config');
```

The supported import forms are `require` (above), named imports and a
namespace import, from an ES module or from TypeScript, and an ES module's
default import under Node's own loader (`.mjs`, `.mts`, `"type": "module"`):

```js
import { producer, common } from '@confluentinc/kafka-javascript';
import { KafkaProducer } from '@confluentinc/kafka-javascript/producer';
import * as kafka from '@confluentinc/kafka-javascript';
```

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
lib/index.ts           the namespaces { producer, common, consumer } and the Java built-in
                       errors (IllegalStateError, IllegalArgumentError, ...)
lib/producer/          KafkaProducer, ProducerRecord, RecordMetadata, Callback (a type),
                       ProducerConfig, BufferExhaustedError
lib/common/            KafkaError, KafkaFuture; errors/ (Java's common.errors), config/
                       (SslConfigs, SaslConfigs, SecurityConfig, SslClientAuth, ConfigError),
                       security/auth/ (SecurityProtocol), ...
lib/consumer/          the consumer's errors only (no consumer yet)
lib/_*.ts              private helpers (not exported, not declared)
dist/                  lib/ compiled by tsc (tsconfig.json): .js, .d.ts and maps, the
                       package's entry points (git-ignored)
src/                   the addon: util.h, addon.c, callback_queue.c, error.c, record.c, producer.c
test/static|unit|typing|integration
```

The addon builds as `build/Release/confluent-kafka-javascript.node`, beside
`dist/`. `lib/_native_loader.ts` loads the platform package's addon first and
falls back to that development build; installing the package runs no build
(`"gypfile": false`, no install scripts).

## Example

```js
const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');

const producer = new KafkaProducer({ 'bootstrap.servers': 'localhost:9092' });
const future = await producer.send(                    // resolves at hand-over
  new ProducerRecord({ topic: 'demo', key: 'k1', value: 'hello' }),
  (metadata, error) => { /* optional Java Callback, runs before the future completes */ });
const metadata = await future.get();                   // resolves at delivery
console.log(metadata.topic, metadata.partition, metadata.offset, metadata.timestamp);
await producer.flush();
await producer.close();          // or close({ timeoutMs })
```

### `send` in two steps

`send(record, callback?)` returns a `Promise<KafkaFuture<RecordMetadata>>`,
the shape of Java's `send()`, which blocks until the record is in the
producer's buffer and then returns a `Future`:

1. The `send()` Promise resolves once the record has been **handed over** to
   the producer: at once, or after waiting up to `max.block.ms` for metadata
   or buffer memory. It rejects with the error Java's `send()` would throw (a
   closed producer, a bootstrap resolution failure, ...); such an error never
   reaches the callback, so do not leave a `send()` Promise unhandled.
2. The future's `get()` resolves at **delivery** with the record's
   `RecordMetadata`, or rejects with the delivery error. The callback, when
   given, runs first, on the JS thread: from the event loop, or inside a
   later `send()` call, which runs the callbacks already due before handing
   its record over. A future whose `get()` is never called raises no
   unhandled rejection. `isDone()` and `isCompletedExceptionally()` tell its
   state; `cancel()` returns `false` (Java's `FutureRecordMetadata`).

**Ordering is the caller's.** Records from code that awaits each `send()`
before the next reach the producer in that order (records to one partition
are appended in send order) and are covered by a following `flush()` or
`close()`: when `flush()` resolves, every such record's future is done.
Sends that are not awaited one by one (e.g. `Promise.all(records.map(r =>
producer.send(r)))`) are concurrent, like Java `send()` calls on several
threads: there is no order between them, and a `flush()` or `close()`
started before they resolved does not wait for them.

**Buffers are borrowed, not copied.** A `Buffer` / `Uint8Array` key or value
must not be modified or detached (transferred) until its `send()` Promise
settles; it may be reused after that. A string key or value is encoded into a
`Buffer` of the client's own and is unaffected.

**Close the producer before teardown.** A producer whose `close()` never
completed is force-closed, as Java's `close(Duration.ZERO)`, when it is
garbage collected, when its worker ends (`worker.terminate()`, or
`process.exit()` inside the worker), or when the main event loop runs out of
work: its unsent records fail, and no JS callback or future runs for them. A
`process.exit()` on the main thread runs no cleanup at all: the process ends
at once and the unsent records are lost the same way. The force-close can still block the thread briefly, while the
client's I/O task finishes a wait it is in (for a broker connection, up to
`socket.connection.setup.timeout.max.ms`). So when records must be delivered,
`await producer.close()` (or at least `flush()`) before ending a worker or the
process; Java's daemon I/O thread loses unsent records at JVM exit the same
way.

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
  'bootstrap.servers': 'broker:9093',
  'security.protocol': 'SASL_SSL',
  'sasl.mechanism': 'PLAIN',
  'sasl.jaas.config':
    'org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="secret";',
  'ssl.truststore.location': '/etc/kafka/ca.pem',
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
error code, so `instanceof` tells them apart. Each has `name` (its class name),
`code` (the C FFI's `kafka_common_Error_e` value), `errorName` (its enumerator
name, e.g. `TOPIC_AUTHORIZATION_FAILED`), `message`, `retriable`, `apiError`
and `cause`. Java's built-in exceptions (`IllegalStateError`,
`IllegalArgumentError`, ...) are at the package root and, as in Java, are not
`KafkaError`s: `send()` on a closed producer throws an `IllegalStateError`. A
config value of the wrong JavaScript type for its key throws a `ConfigError`
with Java's message; any other value of the wrong JavaScript type throws a
`TypeError`, and so does a topic, header key or config key or value holding a
null byte (`"topic must be a string without null bytes"`), which C would read
as the end of the string; keys, values and header values are bytes and may
hold any.

See `CLAUDE.md` for the binding's rules.
