# Confluent Kafka JavaScript (Node.js) client — preview

A Node.js client for Apache Kafka built on the C FFI of the Confluent Kafka
Rust client, in the same way as the Python client in `../python`. It is a
Node-API addon written in plain C (`src/`), so one build works on every Node
release supporting Node-API version 8, under a thin CommonJS layer (`lib/`)
with TypeScript declarations (a `.d.ts` beside every public module, generated
from its JSDoc).

The API follows the Java client's shape (`KafkaProducer`, `ProducerRecord`,
`RecordMetadata`, `KafkaFuture`), with JavaScript idioms (a `Promise` for a
blocking call, a `KafkaFuture` whose `get()` returns a `Promise`). This first
slice covers the producer only; the package is private and not published.

The package is `@confluentinc/kafka-javascript`, the npm name of the
librdkafka-based client, at version `4.4.0-beta.1`: like the Rust-based Python
client, which reuses PyPI's `confluent-kafka` at `4.4.0b1`, it is told apart
from the earlier client (0.x and 1.x releases) by version. It stays `private`:
publishing under the shared name needs the agreement of the team that owns it.

## Requirements

- Node.js 18 or newer, with its headers (official tarballs, Homebrew and Nix
  installs ship them under `<prefix>/include/node`).
- A C compiler and Python 3 (for node-gyp). node-gyp itself is the copy npm
  bundles; no registry access is needed.
- The Rust library and its bindings header:

  ```bash
  make -C rust build-all-features   # rust/target/release/libconfluent_kafka.*
                                    # rust/target/include/confluent_kafka_bindings.h
  ```

## Build and test

```bash
make -C javascript build             # or, from the repository root: make build-javascript
make -C javascript test-static       # header conformance and package layout, no build needed
make -C javascript test-unit         # no broker needed
make -C javascript test-typing       # the .d.ts are up to date and type-check (needs tsc)
make -C javascript test-integration  # starts a scratch broker with docker, or uses
                                     # KAFKA_BOOTSTRAP_SERVERS when it is set
make -C javascript types             # regenerate lib/**/*.d.ts from the JSDoc (needs tsc)
```

`make test` runs all four, then, on Linux, the container arm of the Rust
multilanguage suite (below; it is skipped with a message elsewhere). TypeScript
and `@types/node` are devDependencies (`npm ci` installs them from
`package-lock.json`); `TSC` (default `node_modules/.bin/tsc`) and
`TS_TYPE_ROOTS` (default `node_modules/@types`) point at other copies, e.g.
`make -C javascript types TSC="node /path/to/typescript/bin/tsc" TS_TYPE_ROOTS=/path/to/@types`.
Without a runnable `tsc`, `test-typing` is skipped with a message.

`CONFLUENT_KAFKA_LIB_DIR` (default `../rust/target/release`) and
`CONFLUENT_KAFKA_INCLUDE_DIR` (default `../rust/target/include`) select the
library and header; the addon links the library dynamically with an rpath to
that directory.

### The Rust multilanguage suite

The Rust integration tests' `multilanguage_test!` producer cases also run
against this client, as their `__grpc_js` arm, through `grpc_server.js` (a gRPC
server over `rust/multilanguage-test-server/proto`'s `ProducerService`; it is
not part of the published package):

```bash
make test-integration-javascript-native  # from the repository root: the server as a host process
make test-integration-javascript         # Linux only: the server in a container (Dockerfile.grpc)
```

The native arm needs `@grpc/grpc-js` and `@grpc/proto-loader` (devDependencies);
`make -C javascript grpc-native` installs them under
`rust/target/grpc-native/javascript`, or `MULTILANG_JS_NODE_PATH` names a
`node_modules` that already holds them. The transaction, `partitionsFor` and
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

```
lib/index.js           the namespaces { producer, common, consumer } and the Java built-in
                       errors (IllegalStateError, IllegalArgumentError, ...)
lib/producer/          KafkaProducer, ProducerRecord, RecordMetadata, Callback (a type),
                       ProducerConfig, BufferExhaustedError
lib/common/            KafkaError, KafkaFuture; errors/ (Java's common.errors), config/
                       (SslConfigs, SaslConfigs, SecurityConfig, SslClientAuth, ConfigError),
                       security/auth/ (SecurityProtocol), ...
lib/consumer/          the consumer's errors only (no consumer yet)
lib/_*.js              private helpers (not exported)
src/                   the addon: util.h, addon.c, callback_queue.c, error.c, record.c, producer.c
test/static|unit|typing|integration
```

The addon builds as `build/Release/confluent-kafka-javascript.node`; installing the
package runs no build (`"gypfile": false`, no install scripts).

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
`TypeError`.

See `CLAUDE.md` for the binding's rules.
