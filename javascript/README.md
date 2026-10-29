# Confluent Kafka JavaScript (Node.js) client — preview

A Node.js client for Apache Kafka built on the C FFI of the Confluent Kafka
Rust client, in the same way as the Python client in `../python`. It is a
Node-API addon written in plain C (`src/`), so one build works on every Node
release supporting Node-API version 8, under a thin CommonJS layer (`lib/`)
with TypeScript declarations (`index.d.ts`).

The API follows the Java client's shape (`KafkaProducer`, `ProducerRecord`,
`RecordMetadata`, `KafkaFuture`), with JavaScript idioms (a `Promise` for a
blocking call, a `KafkaFuture` whose `get()` returns a `Promise`). This first
slice covers the producer only; the package is private and not published.

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
make -C javascript test-unit         # no broker needed
make -C javascript test-integration  # starts a scratch broker with docker, or uses
                                     # KAFKA_BOOTSTRAP_SERVERS when it is set
```

`CONFLUENT_KAFKA_LIB_DIR` (default `../rust/target/release`) and
`CONFLUENT_KAFKA_INCLUDE_DIR` (default `../rust/target/include`) select the
library and header; the addon links the library dynamically with an rpath to
that directory.

## Example

```js
const { KafkaProducer, ProducerRecord } = require('confluent-kafka-javascript-rust');

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

## Errors

Every error the client reports is a `KafkaError` (it extends `Error`) with
`code` (the C FFI's `kafka_common_Error_e` value), `name` (its enumerator
name, e.g. `TOPIC_AUTHORIZATION_FAILED`), `message`, `retriable`, `apiError`
and `cause`. A value of the wrong JavaScript type throws a `TypeError`.

See `CLAUDE.md` for the binding's rules.
