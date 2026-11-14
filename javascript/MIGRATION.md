# Migrating to 4.x from 1.x and KafkaJS

`@confluentinc/kafka-javascript` 4.x is a new client under the same npm name:
a Node-API binding over the Confluent Kafka Rust client, whose contract is the
Java client's. It replaces both APIs of 1.x, the KafkaJS-style one
(`require('@confluentinc/kafka-javascript').KafkaJS`) and the node-rdkafka-style
one (`Producer`, `produce()`, events), and it is the client to move to from
KafkaJS. 4.x has one API, shaped like Java's: code written for either API of
1.x, or for KafkaJS, has to change.

This guide lists what users of 1.x and KafkaJS lose and what replaces it: the
call sites to rewrite, what is not offered, what is not available yet, and the
code that keeps running with another meaning. It is not exhaustive. The
preview, `4.4.0-preview.1`, has the **producer only**: the rows about the
consumer and the admin client say what comes with them.

Two rules explain most rows at once:

- **Java's names and Java's shapes.** Classes, methods and parameters are
  Java's (`KafkaProducer`, `ProducerRecord`, `send`, `flush`, `close`), and a
  call takes Java's parameters by name in one object, or, for a form with one
  parameter, the value on its own: `send({ record, callback })`,
  `send(record)`, `close(5000)`, `close({ timeout: 5000 })`. Any other set of
  names throws (or rejects with) `IllegalArgumentError`, listing Java's forms.
- **Java's configuration keys.** The configuration is a plain object of Java's
  dotted keys (`bootstrap.servers`, `linger.ms`, `max.request.size`), not
  librdkafka's names and not a `kafkaJS` block.

## Install

```bash
npm install @confluentinc/kafka-javascript@preview
```

The 4.x pre-releases are published under npm's `preview` dist-tag only; `latest`
stays on the 1.x line until 4.x is generally available, so a plain
`npm install @confluentinc/kafka-javascript`, and a `^1` range, keep installing
1.x. The npm 1.x releases are built from the `2.x` branch of
[confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript),
and their fixes go out under the `legacy` dist-tag.

4.x runs on **Node.js 22.14 or newer**, the long-term-support releases that
have not reached end of life (tested on 22, 24 and 26); the release notes say
when a Node.js release is dropped. 1.x ran on older releases: upgrade Node.js
first. The preview has prebuilt packages for Linux x64 with glibc 2.28 or newer
and for macOS 11 or newer on Apple silicon; elsewhere, build it from source
(`README.md`). Nothing builds or downloads at install.

## Read this first: two changes that keep code running

Both change the result of code that compiles and runs unchanged; check every
producer for them.

1. **`await send()` means accepted, not delivered.** In 1.x's KafkaJS-style
   API and in KafkaJS, `await producer.send({ topic, messages })` waits until
   the broker acknowledges the records and returns their metadata. Here
   `send()` resolves once the producer has **accepted** the record, with a
   `KafkaFuture`, and `await future.get()` waits for **delivery**:

   ```js
   const future = await producer.send(record);   // accepted
   const metadata = await future.get();          // delivered, or rejects
   ```

   Code that ignores the result of `await send()` (an app that commits its
   consumer offsets once `send()` resolved, or logs "sent") no longer knows
   that the record was delivered: a send that fails later fails silently for
   it. TypeScript flags code that reads the result, now a `KafkaFuture`, but
   not code that ignores it. Await `future.get()`, pass a `callback`, or
   `await producer.flush()` before relying on delivery.

2. **The default partitioner hashes keys with CRC-32.** A record with a key
   goes to `CRC-32(key) mod partitions`, as librdkafka's `consistent_random`,
   1.x's node-rdkafka-style default, does. 1.x's KafkaJS-style API and KafkaJS
   (like the Java client) hash with **murmur2**, so the same key lands on
   another partition: ordering by key, and anything that assumes which
   partition holds a key, breaks without an error. To keep the partitions of
   a KafkaJS-style producer, or of a Java one, set:

   ```js
   new KafkaProducer({ 'bootstrap.servers': 'broker:9092', 'partitioner.type': 'Murmur2RandomPartitioner' });
   ```

   `partitioner.type` takes `ConsistentRandomPartitioner` (the default),
   `Murmur2RandomPartitioner` or `RoundRobinPartitioner`. A record without a
   key goes to a sticky partition chosen by load (Java's KIP-794 behaviour),
   not to a random one per record. (Whether the default stays CRC-32 is not
   decided yet.)

## 1. Rewriting call sites

### Construction and lifecycle

| 1.x / KafkaJS | 4.x | Note |
|---|---|---|
| `new Kafka({ kafkaJS: { brokers } }).producer({ kafkaJS: { ... } })` | `new KafkaProducer({ 'bootstrap.servers': 'host:9092', ... })`, or `new KafkaProducer({ configs })` | Imported from its package path: `require('@confluentinc/kafka-javascript/producer')`. A `kafkaJS` block is not read: it reaches the core as an unknown key, which the core lists as unused |
| `new Kafka.Producer(conf)` (node-rdkafka style) | `new KafkaProducer(configs)` | Java's keys (Configuration, below) |
| `await producer.connect()` | nothing: a client is ready once it is constructed, as in Java | There is no `connect()`; calling it is a `TypeError` |
| `await producer.disconnect()` | `await producer.close()`, or `await using producer = new KafkaProducer(...)` | `close()` waits without a time limit for the records already sent (Java's `close()`); `close(0)` or `close({ timeout: 0 })` does not wait for them. As 1.x and KafkaJS from `connect()` to `disconnect()`, a producer keeps Node running from its creation until `close()` closes it: a script that never closes it never exits |
| `producer.disconnect(timeout, cb)` | `await producer.close(timeout)` | A timeout is a number of milliseconds or a `Temporal.Duration`; `0` means "don't wait", `Infinity` no limit; a negative one throws `IllegalArgumentError` |
| events: `producer.on('ready' / 'event.error' / 'delivery-report' / 'disconnected', ...)` | none | There is no event emitter. An operation's error rejects its Promise or fails its future; a record's delivery goes to its `callback` and its future |
| `logger`, `logLevel`, `log_level`, `debug` options, `event.log` | none | No config value is a callback or a logger. The Rust core logs to stderr under `RUST_LOG`; the binding's own debug lines show with `NODE_DEBUG=confluent_kafka`; its warnings use `process.emitWarning` |

### Producing

| 1.x / KafkaJS | 4.x | Note |
|---|---|---|
| `producer.send({ topic, messages: [{ key, value, partition, headers, timestamp }] })` | one `send()` per record: `send(new ProducerRecord({ topic, partition, timestamp, key, value, headers }))` | A record is a class instance built with Java's constructors; a plain `{ topic, messages }` object is refused with `IllegalArgumentError` (send's forms are `(record)` and `(record, callback)`) |
| `producer.sendBatch({ topicMessages })` | `send()` in a loop | The core batches records itself |
| `producer.produce(topic, partition, value, key, timestamp, opaque, headers)` (node-rdkafka style) | `send({ record: new ProducerRecord({ ... }), callback })` | The `opaque` is a closure over whatever the callback needs |
| a record field left out or `undefined`: `{ topic, value, headers }` | the fields of one of Java's six `ProducerRecord` constructors, e.g. `{ topic, partition: null, key, value, headers }` | A record with headers or a timestamp names its `partition`; `null` lets the producer choose. `{ topic, partition, value }` is no Java constructor. `undefined` is never a value: leave the key out, or pass `null` (Java's null; a `null` value is a tombstone) |
| `partition: -1` ("any"), `timestamp: 0` or `Date.now()` strings | `partition: null`, `timestamp: null` (the producer's clock) | A negative partition or timestamp throws `IllegalArgumentError`, with Java's message; `timestamp: 0` is the literal epoch |
| headers as an object `{ k: 'v' }` or `[key, value]` pairs, string values | `[new Header({ key: 'k', value: Buffer.from('v') })]` (`common/header`), or a `Headers` | Header values are bytes or `null`: encode a string first. Header keys are strings |
| the delivery report `(err, report)`, run by `poll()` / `setPollInterval()` | the callback `({ metadata, exception }) => ...`, one object with Java's parameter names | It runs on the JavaScript thread, before the record's future completes, without any polling: delete `producer.poll()` and `setPollInterval()`. On failure `metadata` has Java's `-1` fields. It may return a Promise, which the producer awaits |
| `dr_cb: true`, `dr_msg_cb` | the `callback` of each `send()` | Per send, not per client |
| `acks`, `timeout`, `compression` per `send()` (KafkaJS) | the producer's `acks`, `delivery.timeout.ms` and `compression.type` configuration | Java sets them per producer |
| `await producer.flush({ timeout })` / `flush(timeout, cb)` | `await producer.flush()` | Java's `flush()` takes no timeout: it waits until every record sent before it is delivered or failed |
| `producer.transaction()`, `sendOffsets()`, `commit()`, `abort()` (KafkaJS) | `initTransactions()`, `beginTransaction()`, `sendOffsetsToTransaction()`, `commitTransaction()` and `abortTransaction()`, Java's names | Not in the preview yet (Section 3). When they come, they return Promises, `beginTransaction()` included, and a send counts in a transaction once its `send()` Promise resolved: await every `send()` before `commitTransaction()` |

### Reading results and values

| 1.x / KafkaJS | 4.x | Note |
|---|---|---|
| `RecordMetadata[]` from `send()`: `{ topicName, partition, baseOffset, errorCode }` | `RecordMetadata` from `future.get()`: `metadata.topic`, `.partition`, `.offset`, `.hasOffset`, `.timestamp`, `.serializedKeySize`, ... | A **data object**: Java's getters are `readonly` fields, so they read without `()`. **Silent:** `offset` and `timestamp`, Java's `long`, are bigints (`metadata.offset === 0n`), where 1.x's callback API gives numbers and its KafkaJS-style API strings; `JSON.stringify` writes them as decimal strings |
| other reads | methods, with `()`: `header.key()`, `header.value()`, `future.isDone()`, `SecurityProtocol.SSL.name()` | Only the data objects (`ProducerRecord`, `RecordMetadata`; on the consumer side the records and `TopicPartition`) read through fields. Without the `()` a read returns the function itself and raises no error |
| `err.code`, `err.type`, `err.retriable`, `KafkaJSError` subclasses | Java's classes, `Exception` read as `Error`: `instanceof RetriableError`, `instanceof TopicAuthorizationError`, `err.name` as the fallback | An error has no `code` or `retriable` field; they read `undefined`. The classes follow Java's `extends` chain and are in their packages (`common/errors`, `producer`, ...) |
| a failed `await send()` rejected with the delivery error | `send()` rejects with what Java's `send()` throws (a closed producer, a metadata timeout); `future.get()` rejects with an `ExecutionError` whose `cause` is the delivery error | Only `get()` wraps the failure, as Java's `Future.get()` does; the callback's `exception` and `toCompletionStage()` give the error itself |
| deep imports: `@confluentinc/kafka-javascript/types/kafkajs`, `.../lib/...` | each package from its own path: `@confluentinc/kafka-javascript/producer`, `/common`, `/common/errors`, `/common/header`, `/common/config`, ... | A deep import fails with Node's `ERR_PACKAGE_PATH_NOT_EXPORTED`. The package root exports only Java's built-in errors and the `Duration` type |

### Configuration

The configuration is a plain object of Java's keys (a `Map` throws
`TypeError`). librdkafka's names, and the `kafkaJS` block's options, are not
read: a librdkafka key the client does not know reaches the core as an unknown
key, which the core lists in one log line and otherwise ignores.

| librdkafka (1.x) | KafkaJS / 1.x KafkaJS-style | 4.x (Java) |
|---|---|---|
| `metadata.broker.list`, `bootstrap.servers` | `brokers: [...]` | `bootstrap.servers: 'a:9092,b:9092'` (or an array) |
| `client.id` | `clientId` | `client.id` |
| `queue.buffering.max.ms`, `linger.ms` | — | `linger.ms` |
| `batch.num.messages` | — | `batch.size` (bytes, not records) |
| `message.max.bytes` | — | `max.request.size` |
| `compression.codec` | `compression` per send | `compression.type` |
| `request.required.acks` | `acks` per send | `acks` (`'all'`, `0`, `1`) |
| `message.timeout.ms` | `timeout` per send | `delivery.timeout.ms` |
| `enable.idempotence` | `idempotent` | `enable.idempotence` (on by default, as in Java) |
| `max.in.flight` | `maxInFlightRequests` | `max.in.flight.requests.per.connection` |
| `socket.connection.setup.timeout.ms` | `connectionTimeout` | `socket.connection.setup.timeout.ms` |
| `request.timeout.ms` | `requestTimeout` | `request.timeout.ms` |
| `retries`, `retry.backoff.ms` | `retry: { retries, initialRetryTime }` | `retries`, `retry.backoff.ms`, `retry.backoff.max.ms` |
| `security.protocol` | `ssl: true`, `sasl: { ... }` | `security.protocol` (`SSL`, `SASL_SSL`, ...) |
| `ssl.ca.location` | `ssl: { ca }` | `ssl.truststore.location` (a PEM file) or `ssl.truststore.certificates` (PEM text) |
| `ssl.certificate.location`, `ssl.key.location` | `ssl: { cert, key }` | `ssl.keystore.location` (one PEM file with the chain and the key), or `ssl.keystore.certificate.chain` and `ssl.keystore.key` |
| `sasl.mechanisms`, `sasl.username`, `sasl.password` | `sasl: { mechanism, username, password }` | `sasl.mechanism` and `sasl.jaas.config` (`org.apache.kafka.common.security.plain.PlainLoginModule required username="..." password="...";`); the preview supports `PLAIN` only |
| `partitioner` | `createPartitioner` | `partitioner.type` (Read this first, above) |

The config constants are Java's: `ProducerConfig.LINGER_MS_CONFIG`,
`SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG`, `SaslConfigs.SASL_JAAS_CONFIG`.
Every value is written as a string (`true` as `"true"`, numbers and bigints
in decimal, arrays joined with `,`, anything else by `String()`), and the
core checks it, throwing `ConfigError` with Java's message naming the key;
`null` and `undefined` leave a key unset.

**1.x's serializer builder keys.** `js.key.serializer.builder` and
`js.value.serializer.builder` (1.x's Schema Registry serde builders) are not
keys of this client: their value, an object, fails the configuration check
with `ConfigError`. Java's `key.serializer` and `value.serializer` throw
`ConfigError` too, whatever their value: a serializer is a constructor
argument, never a configuration value. The preview has no serializers yet
(Section 3): serialize keys and values yourself, then send the bytes.

**Schema Registry.** Keep `@confluentinc/schemaregistry`, its imports and its
calls, and upgrade it to its release that supports 4.x, which ships its own
copy of the client types and takes the one-object serde call. Older releases
import 1.x's `@confluentinc/kafka-javascript/types/kafkajs`, so a TypeScript
build that uses them with 4.x fails with `TS2307: Cannot find module` (or gets
`any` types under `skipLibCheck`). Serializing by hand and sending the bytes
keeps working with any release:

```js
const value = await serializer.serialize(topic, { fullName: 'John Doe' });
await (await producer.send(new ProducerRecord({ topic, key: 'k', value }))).get();
```

### Calls and arguments

| 1.x / KafkaJS | 4.x | Note |
|---|---|---|
| positional arguments, `send(record, callback)` | one object of Java's names, `send({ record, callback })`, or the value on its own for a one-parameter form, `send(record)`, `close(5000)` | A second argument is refused with `IllegalArgumentError`, never dropped |
| `undefined` for "not given" | leave the key or the argument out; `null` is Java's null | `undefined` as a value throws `TypeError` naming the parameter (`"value must not be undefined. Pass null for Java's null, which sends a tombstone."`), except in a configuration, where it leaves the key out |
| timeouts in milliseconds, `-1` for "forever" | milliseconds or a `Temporal.Duration`; `Infinity` for no limit, `0` for "don't wait" | A negative timeout throws `IllegalArgumentError` |
| keys and values: `Buffer`, string | `Uint8Array` (a `Buffer` is one) or a string | Not copied: don't write to, transfer or resize a key or value until the Promise from `send()` settles (1.x copies a plain Buffer inside `send()`); awaiting `send()` is enough, and a serializer returns a new Buffer for each record. A key or value read back from the client is a `Buffer` |

## 2. Not offered, by design

| 1.x / KafkaJS | Why | Instead |
|---|---|---|
| a KafkaJS-style API in the same package | 4.x has one API, Java's | the Java-shaped API of this guide |
| `connect()`, `disconnect()`, `run()` helpers, events | a client is ready once constructed, as in Java; Java has no event emitter | construct it; `close()`; the callback and the futures |
| a producer `poll()` / `setPollInterval()` | callbacks run on the JavaScript thread without polling | delete the call |
| `error_cb`, `dr_cb`, `stats_cb`, `oauthbearer_token_refresh_cb`, `rebalance_cb`, `offset_commit_cb` config callbacks | no config value is a callback, as in Java | an operation's error rejects its call or fails its future; deliveries go to each `send()`'s callback; 1.x's `rebalance_cb` becomes a listener passed to `subscribe()`, and `offset_commit_cb` the commit call's callback (with the consumer). A custom OAuth token callback has no replacement until the core has a token hook |
| a `logger` option | the core logs on its own | `RUST_LOG` for the core, `NODE_DEBUG=confluent_kafka` for the binding |
| `producer.sendBatch()`, a producer queue length | the core batches and applies backpressure itself (`max.block.ms`) | `send()` in a loop, then `flush()` |
| 1.x's `types/kafkajs` type definitions | the package ships the types of its own API | import the types from the package paths |
| more than one copy of the package in an app | classes are told apart with plain `instanceof` | install it once (`npm ls @confluentinc/kafka-javascript`, `npm dedupe`): "The client supports one copy of the package in an app. If `node_modules` holds two versions, calls that take the client's classes can fail." |
| sharing a client or its objects between worker threads | a client stays in the thread that created it | "The client doesn't support sharing its objects between worker threads. Create one client per thread, and send plain data between threads." (`README.md`, Limitations, shows how to turn a record and its `Headers` into plain data) |

## 3. Not available yet

| Feature | Comes with |
|---|---|
| The consumer: `KafkaConsumer` with `poll()`, `subscribe()`, `assign()`, `commitSync()` and `commitAsync()` | the consumer |
| The admin client: `Admin.create()`, its RPCs, `*Options` and `*Result` classes | the admin client |
| Transactions: `initTransactions()`, `beginTransaction()`, `sendOffsetsToTransaction()`, `commitTransaction()`, `abortTransaction()` | not translated yet |
| `partitionsFor()`, `metrics()`, `clientInstanceId()` | not translated yet |
| Serializers: the `keySerializer` / `valueSerializer` constructor arguments and the built-in factories (`stringSerializer()`, ...) | the serdes |
| Schema Registry | the release of `@confluentinc/schemaregistry` that supports 4.x |
| Musl (Alpine), Linux arm64 and Windows packages | later releases; build from source until then |

When the consumer and the admin client come, this guide gains their rows. What
is decided for them already:

- **Listeners must finish, and must await what they start.** A rebalance
  listener (`onPartitionsRevoked`, `onPartitionsAssigned`, `onPartitionsLost`)
  takes one object, `onPartitionsRevoked({ partitions })`; the rebalance does
  not go on until the listener, and the Promise it returns, settles, and a
  listener that never settles blocks its `poll()` without a time limit.
- **Stop the poll loop before `close()`.** `close()` during a pending
  `poll()` fails, as in Java: call `wakeup()`, let the loop exit, then
  `await consumer.close()`.
- **`commitSync()` and `commitAsync()` keep Java's names, and both return
  Promises**: only `commitSync()` waits for the broker; `commitAsync()`
  settles once the core has taken the commit, and its `callback` gets the
  broker's answer.
- **A pattern is a `SubscriptionPattern`, not a `RegExp`**: the broker
  matches the whole topic name, so `/^orders-/` becomes
  `new SubscriptionPattern({ pattern: 'orders-.*' })`.
- **A map keyed by partition is a `TopicPartitionMap`**, whose `keys()` are
  `TopicPartition` objects (`tp.topic`, `tp.partition`), not a `Map` of
  strings.
- **Every consumer sets `group.protocol: 'consumer'`** (KIP-848) until the
  core changes its default.
- **The admin results and options are classes** (`new CreateTopicsOptions({
  timeoutMs: 5000 })`, `result.values()`), each RPC returning its `*Result` at
  once, with a `KafkaFuture` per key; and **`close()` has no time limit on the
  admin client either**.
- **Serdes go in the constructor**, never in the configuration, and a string
  key or value then needs `stringSerializer()`.

## 4. Available here and not in 1.x or KafkaJS

- Java's `KafkaFuture` for every send: `get()`, `get(timeout)`, `getNow()`,
  `thenApply()`, `whenComplete()`, `toCompletionStage()`, `allOf()`,
  `completedFuture()`, `isDone()`; a future nobody waits on raises no
  unhandled rejection.
- Java's error hierarchy as classes, with `cause` chains, checked with
  `instanceof`, and Java's messages word for word.
- TypeScript declarations of every form: one overload signature per Java form,
  so `tsc` rejects a set of parameters Java has no form for.
- `await using` closes a producer (`[Symbol.asyncDispose]`).
- `Temporal.Duration` timeouts, beside milliseconds.
- Java's config constants (`ProducerConfig`, `SslConfigs`, `SaslConfigs`,
  `SecurityConfig`) with Java's documentation strings.
- A delivery callback that returns a Promise, which the producer awaits before
  the record's future completes.

## 5. Same code, different result

These need no code change, which is why they are dangerous: the same source
behaves otherwise.

| Behaviour | 1.x / KafkaJS | 4.x | Consequence on port |
|---|---|---|---|
| `await send()` | resolves on delivery, with the metadata (KafkaJS-style API, KafkaJS) | resolves on acceptance, with a `KafkaFuture` | Code that treats "resolved" as "delivered" loses records silently on a later failure (Read this first) |
| Keyed record → partition | murmur2 (KafkaJS-style API, KafkaJS); CRC-32 (node-rdkafka style) | CRC-32 | The same key lands on another partition; set `partitioner.type: 'Murmur2RandomPartitioner'` (Read this first) |
| Keyless record → partition | round-robin or random | a sticky partition chosen by load (KIP-794) | Another distribution and batching |
| A lone surrogate in a string key, value, header key or config value | written as U+FFFD (`Buffer.from`) | written as `?`, as Java's `String.getBytes(UTF_8)` writes it | Other bytes on the wire for a malformed string; well-formed strings encode the same |
| `enable.idempotence` unset | off (librdkafka, KafkaJS) | on, as in Java, unless a conflicting setting turns it off | `acks` is `all` and retries keep each partition's order, as Java's producer does by default |
| `close()` with no timeout | — | waits without a time limit for the records already sent, as Java's `close()` | Shut down with `close(timeout)` when the process must end in time |
| A delivery callback that throws, or whose Promise rejects | — | the producer logs it, as Java's does, and it reaches no caller; the record's future still completes with its delivery result | Report errors from the callback yourself |
| Inside a callback: `flush()`, `close()`, a transaction call, or awaiting a later record's future | — | each waits for the callback itself: the producer refuses them before the callback's first `await`, and after it they can hang | Do not wait on the same producer from inside its callback |
| A callback Promise that never settles | — | has no time limit, as in Java: it holds its record's future, `flush()` and `close()`, `close({ timeout })` included | Make every callback settle |
| A Rust panic in the core | — | reaches the caller unchanged, as the error of the call it happened in | Close that client and create a new one |
| `TimeoutError` | one class | two: the root's (Java's `java.util.concurrent.TimeoutException`), which a timed `future.get(timeout)` rejects with, and `common/errors`' Kafka `TimeoutError`, which a delivery or `max.block.ms` timeout fails a record with (the `cause` of `get()`'s `ExecutionError`, and the callback's `exception`) | Check the class you mean; neither is the `DOMException` of `AbortSignal.timeout()` |
| String keys and values without a serializer | strings sent as UTF-8 | the preview encodes them as Java's `StringSerializer` does; with the serdes, a string will need `stringSerializer()` | Pass `Uint8Array`s, or plan for `stringSerializer()` (Section 3) |

`README.md` is the reference for the rest of the producer's API, and
`CHANGELOG.md` lists each change in the release where it lands.
