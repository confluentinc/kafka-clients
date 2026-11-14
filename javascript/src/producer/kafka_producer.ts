// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Java's KafkaProducer over the C FFI's KafkaProducer with NULL serializers:
// keys and values cross as bytes (javascript/CLAUDE.md); the serializers the
// producer is given run here, in JavaScript, inside send() (root CLAUDE.md,
// JavaScript Binding Conventions, Serialization).

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('../internal/native');
import { JavaForms } from '../internal/args';
import { configEntries } from '../internal/config';
import { debug } from '../internal/debug';
import { durationToMillis, isDurationOrNull } from '../internal/duration';
import { producerClosed, producerOpened, warnPreview } from '../internal/lifecycle';
import { checkSerializer, closeSerializer, isThenable, serializedBytes } from '../internal/serde';
import { KafkaError } from '../common/kafka_error';
import { KafkaFuture } from '../common/kafka_future';
import { Node } from '../common/node';
import { PartitionInfo } from '../common/partition_info';
import type { Serializer } from '../common/serialization/serializer';
import type { Duration } from '../duration';
import type { Callback } from './callback';
import type { Producer } from './producer';
import type { ProducerConfigs } from './producer_configs';
import { ProducerRecord } from './producer_record';
import type { RecordMetadata } from './record_metadata';

/**
 * The parameters of {@link KafkaProducer.close}'s timed form, Java's
 * `close(Duration)`.
 *
 * @remarks *(js-only)* The object of named parameters of Java's
 * `close(Duration)`; Java's `close()` takes none.
 */
export type CloseArgs = {
  /**
   * The maximum time to wait for the producer to complete any pending
   * requests: a number of milliseconds (`Infinity`: no limit), or a
   * `Temporal.Duration`, rounded down to whole milliseconds. The value
   * should be non-negative. Specifying a timeout of zero means do not wait
   * for pending send requests to complete.
   */
  timeout: Duration;
};

/** The native producer handle (`internal/native.ts`). */
type ProducerHandle = ReturnType<typeof native.producerCreate>;

// Java's forms of each method, in Java declaration order (src/internal/args.ts). The
// constructor's Map and Properties forms are the one `configs`, which is also
// taken bare, the configuration map itself, and so are its two forms taking
// serializers.
// A bare value picks its form by its type: the configs an object (a `Map` is
// then refused as the configuration), the record a ProducerRecord, the timeout
// a number or a Temporal.Duration; `null` is Java's null.
const CONSTRUCTOR_FORMS = new JavaForms('KafkaProducer', [['configs'], ['configs', 'keySerializer', 'valueSerializer']], {
  bareIsPlainObject: true,
  bareTypes: { configs: (value) => typeof value === 'object' },
});
const SEND_FORMS = new JavaForms('send', [['record'], ['record', 'callback']], {
  bareTypes: { record: (value) => value === null || value instanceof ProducerRecord },
});
const CLOSE_FORMS = new JavaForms('close', [[], ['timeout']], { bareTypes: { timeout: isDurationOrNull } });
const FLUSH_FORMS = new JavaForms('flush', [[]]);
const PARTITIONS_FOR_FORMS = new JavaForms('partitionsFor', [['topic']], {
  bareTypes: { topic: (value) => value === null || typeof value === 'string' },
});

/** The native layer's node fields as a Node, Java's five-parameter constructor. */
function nodeOf(fields: { id: number; host: string | null; port: number; rack: string | null; isFenced: boolean } | null): Node | null {
  return fields === null ? null : new Node(fields);
}

/** send()'s mark of a value its serializer has not run on yet. */
const NOT_SERIALIZED: unique symbol = Symbol('not serialized');

// A send() the producer takes at once settles its Promise inside the call, so
// awaiting it runs only microtasks: a loop that awaits such sends never returns
// to the event loop, where timers, I/O and the threadsafe function's callback
// drain run, and every record's delivery state piles up until it ends. After
// this many such sends since the event loop last turned, every further one
// settles its Promise only once a turn has run: the first schedules one
// setImmediate, which settles them all in order and restarts the count
// (Python's AsyncProducer._FAST_PATH_YIELD_EVERY). Whichever of them the caller
// awaits, its await then waits for that turn. Their records are handed over at
// once all the same: only the resolutions wait.
const FAST_PATH_YIELD_EVERY = 1000;
// Every this many such sends (the count goes on during the deferral), and on
// the first send after one whose hand-over had to wait (which restarts the
// count), send() runs the callback drain itself on entry, so delivery
// callbacks run and their state is released in batches of at most this many
// records (Python's AsyncProducer._PUMP_EVERY).
const PUMP_EVERY = 64;

// The core's relabelled failure of the producer's construction, Java's
// KafkaException("Failed to construct kafka producer", t).
const CONSTRUCTION_FAILED = 'Failed to construct kafka producer';

/** Whether `error` is the construction failure Java's constructor throws from its catch. */
function isConstructionFailure(error: unknown): boolean {
  return error instanceof KafkaError && error.constructor === KafkaError && error.message === CONSTRUCTION_FAILED;
}

/** The callback `send` was given, or null; throws a TypeError for a non-function. */
function checkCallback(callback: unknown): Callback | null {
  if (callback === undefined || callback === null) return null;
  if (typeof callback !== 'function') throw new TypeError('callback must be a function ({ metadata, exception }) => void');
  return callback as Callback;
}

/**
 * Records a record's delivery on its future after its callback ran, which
 * completes it (Java completes the future after onCompletion returned), and
 * gives back `failure`, the text of what the callback threw, for the core to
 * log.
 */
function completeRecord(
  future: KafkaFuture<RecordMetadata>,
  metadata: RecordMetadata,
  error: Error | null,
  failure: string | undefined,
): string | undefined {
  future.internalSetDelivery(metadata, error);
  return failure;
}

/**
 * `Promise.resolve(returned)`, the callback's result, with the record's
 * delivery recorded once it settled: resolving to undefined, or to the text of
 * the rejection, which the producer reports as a throw. It never rejects (a
 * `then` getter that throws is a rejection too).
 */
function completeWhenSettled(
  returned: unknown,
  future: KafkaFuture<RecordMetadata>,
  metadata: RecordMetadata,
  error: Error | null,
): Promise<string | undefined> {
  return Promise.resolve(returned).then(
    () => completeRecord(future, metadata, error, undefined),
    (reason: unknown) => completeRecord(future, metadata, error, thrownMessage(reason)),
  );
}

/**
 * The text the core logs for a callback that threw (Java logs a throwing
 * onCompletion). It never throws, whatever was thrown (a revoked Proxy, an
 * Error whose `name` or `message` getter throws), so the record's future is
 * always completed.
 */
function thrownMessage(thrown: unknown): string {
  try {
    if (thrown instanceof Error) return `${String(thrown.name)}: ${String(thrown.message)}`;
    return String(thrown);
  } catch {
    return 'the callback threw a value that cannot be converted to a string';
  }
}

/**
 * A Kafka client that publishes records to the Kafka cluster.
 *
 * @remarks The producer is thread safe in Java; here every call is made from
 * the JS thread, and the delivery callbacks run there too: from the event
 * loop, or synchronously inside a later `send()` call, which runs those
 * already due before handing its record over (so a callback may run while the
 * code that called `send()` is mid-call). Records are batched and sent by the
 * client in the background; `send` resolves once the record has reached the
 * producer, with a future of the record's metadata. A producer keeps Node
 * running from its creation until `close()` closes it, and no garbage
 * collection closes one: close every producer (`close()`, or `await using`).
 * *(deviation: Java's client threads are daemon threads, so a program can
 * exit with a client still open. Why: Node exits in the middle of an `await`
 * when nothing holds it, so this way no pending send is cut off.)*
 */
export class KafkaProducer<K = Uint8Array, V = Uint8Array> implements Producer<K, V> {
  /** The name prefix of the producer's network thread. */
  static readonly NETWORK_THREAD_PREFIX: string = 'kafka-producer-network-thread';

  /** The group of the producer's metrics. */
  static readonly PRODUCER_METRIC_GROUP_NAME: string = 'producer-metrics';

  #handle: ProducerHandle;
  // The serializers given, or null: the record's bytes cross as they are.
  // (Typed over unknown: send() reads the record as ProducerRecord<unknown, unknown>.)
  readonly #keySerializer: Serializer<unknown> | null;
  readonly #valueSerializer: Serializer<unknown> | null;
  // Sends whose hand-over completed inside the call since the event loop last
  // turned (as far as this producer saw: the deferral's setImmediate).
  #fastPathSends = 0;
  // Set by a send whose hand-over had to wait: the next send runs the queued
  // callbacks first (root conventions, Threads and callbacks). It leaves the
  // count alone, since a queued hand-over may complete inside another send's
  // pump with no event-loop turn.
  #pumpNext = false;
  // Settlements waiting for the event-loop turn, or null when none is scheduled:
  // each a settle function, then the value it settles with.
  #deferred: unknown[] | null = null;

  /**
   * A producer is instantiated by providing a set of key-value pairs as
   * configuration. Valid configuration strings are documented in Java's
   * ProducerConfig.
   *
   * @param options - the producer configs
   * @throws `KafkaError` when the configuration is invalid (`ConfigError`,
   *   the core's, with Java's message naming the key; also for
   *   `key.serializer` and `value.serializer`) or the producer cannot be
   *   built
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{configs}` or `{configs, keySerializer, valueSerializer}`, the configs
   *   are not an object, or a serializer is a function that makes one
   *   ("did you forget '()'?")
   * @throws `TypeError` when `configs` is null, `undefined` or not a plain
   *   object (a `Map` included), a config key or value holds a null byte,
   *   which the C ABI would read as the end of the string, or a serializer is
   *   neither null nor an object with a `serialize` method
   *
   * @remarks Java's `Map` and `Properties` constructors are the one
   * `configs`. Without serializers a key or value is sent as the bytes the
   * record holds, and the producer is a `KafkaProducer<Uint8Array,
   * Uint8Array>`. The first client a process builds emits one
   * `ExperimentalWarning` while the client is in preview.
   */
  constructor(options: {
    /** The producer configs: a plain object of Java's ProducerConfig keys. */
    configs: ProducerConfigs;
    keySerializer?: never;
    valueSerializer?: never;
  });
  /**
   * A producer is instantiated by providing a set of key-value pairs as
   * configuration, a key and a value `Serializer`. Valid configuration
   * strings are documented in Java's ProducerConfig.
   *
   * @param options - the producer configs and the serializers
   *
   * @remarks The serializers run in JavaScript, inside `send()`; a serializer
   * whose `serialize` returns a Promise is awaited before the record is handed
   * over. A null serializer sends the record's bytes as they are, binding
   * `Uint8Array`. The producer closes both serializers when it closes.
   */
  constructor(options: {
    /** The producer configs: a plain object of Java's ProducerConfig keys. */
    configs: ProducerConfigs;
    /**
     * The serializer for key that implements `Serializer`, or null. The
     * configure() method won't be called in the producer when the serializer
     * is passed in directly.
     */
    keySerializer: Serializer<K> | null;
    /**
     * The serializer for value that implements `Serializer`, or null. The
     * configure() method won't be called in the producer when the serializer
     * is passed in directly.
     */
    valueSerializer: Serializer<V> | null;
  });
  /**
   * A producer is instantiated by providing a set of key-value pairs as
   * configuration: the configs on their own, the same as `{configs}`. A plain
   * object is read as `{configs}` when it has the key `configs`.
   *
   * @param configs - The producer configs
   */
  constructor(configs: ProducerConfigs);
  constructor(options: unknown) {
    const form = CONSTRUCTOR_FORMS.match(options, arguments.length);
    const entries = configEntries(CONSTRUCTOR_FORMS.value(options, 'configs'));
    this.#keySerializer = form === 0 ? null : checkSerializer(CONSTRUCTOR_FORMS.value(options, 'keySerializer'), 'keySerializer');
    this.#valueSerializer = form === 0 ? null : checkSerializer(CONSTRUCTOR_FORMS.value(options, 'valueSerializer'), 'valueSerializer');
    try {
      this.#handle = native.producerCreate(entries);
    } catch (error) {
      // Java's constructor closes the serializers in the catch around its
      // body, whose failure it relabels "Failed to construct kafka producer"
      // (the core's KafkaError). A ConfigError of the configuration's check
      // is thrown before that catch: the serializers stay open, as in Java.
      if (isConstructionFailure(error)) this.#closeSerializers();
      throw error;
    }
    producerOpened();
    warnPreview();
    debug('KafkaProducer created');
  }

  // Java's close closes both serializers; a failure is a process warning.
  #closeSerializers(): void {
    closeSerializer(this.#keySerializer, 'keySerializer');
    closeSerializer(this.#valueSerializer, 'valueSerializer');
  }

  /**
   * Asynchronously send a record to a topic and invoke the provided callback
   * when the send has been acknowledged.
   *
   * The send is asynchronous: the returned Promise resolves once the record
   * has been stored in the buffer of records waiting to be sent, with the
   * future of the record's metadata. This allows sending many records in
   * parallel without blocking to wait for the response after each one.
   *
   * @param options - the record, and the callback to execute when it has
   *   been acknowledged
   * @returns resolving at hand-over to the future of the record's metadata,
   *   rejecting with the error Java's send() would throw
   * @throws `IllegalStateError` rejects with it when the producer is closed,
   *   the core's error with Java's message
   * @throws `IllegalArgumentError` rejects with it when the given parameters
   *   are not `{record}` or `{record, callback}`, or a bare value is not a
   *   ProducerRecord
   * @throws `TypeError` rejects with it when `record` is null or not a
   *   ProducerRecord, `callback` not a function, a parameter is `undefined`,
   *   a serializer returns neither a Uint8Array nor null, a key or value
   *   sent without a serializer is neither bytes, a string nor null, or the
   *   topic or a header key holds a null byte, which the C ABI would
   *   read as the end of the string
   * @throws `InterruptError` rejects with it if the thread is interrupted while
   *   blocked
   * @throws `SerializationError` rejects with it if the key or value are not
   *   valid objects given the configured serializers (what a serializer
   *   throws, or rejects with, rejects the send as it is)
   * @throws `BootstrapResolutionError` rejects with it if DNS resolution of the
   *   bootstrap servers fails within `bootstrap.resolve.timeout.ms`
   * @throws `KafkaError` rejects with it if a Kafka related error occurs that
   *   does not belong to the public API errors
   *
   * @remarks Java's blocking `send()` returning a `Future` is two steps here:
   *
   * 1. The returned Promise resolves once the record has been handed to the
   *    producer (where Java's `send()` returns: at once, or after waiting up
   *    to `max.block.ms` for metadata or buffer memory), with a
   *    {@link KafkaFuture} of the record's metadata. It rejects with the error
   *    Java's `send()` would throw (a closed producer, a bootstrap resolution
   *    failure, ...); such an error never reaches the callback.
   * 2. The future's `get()` resolves to the RecordMetadata assigned to the
   *    record once the record is acknowledged, or rejects with an
   *    `ExecutionError` whose `cause` is the error the delivery failed with,
   *    as Java's `get()` throws. The callback, when given, runs first, on the
   *    JS thread, and receives the error itself; a callback that returns a
   *    Promise is awaited before the future completes. A future whose `get()`
   *    is never called raises no unhandled rejection.
   *
   * Ordering is the caller's: records from code that awaits each `send()`
   * before the next reach the producer in that order (so records to one
   * partition are appended in that order) and are covered by a following
   * `flush()` or `close()`. Sends that are not awaited one by one (e.g. under
   * `Promise.all`) are concurrent, like Java `send()` calls on several
   * threads: there is no order between them, and a `flush()` or `close()`
   * started before they resolved does not wait for them.
   *
   * A loop awaiting sends the producer takes at once still lets the event loop
   * run: after 1000 such sends in a row, such sends resolve only after one
   * event-loop turn, whichever of them is awaited (their records are handed
   * over at once all the same), and every 64th runs the delivery callbacks
   * already due before handing its record over, so a callback may run inside
   * a `send()` call.
   *
   * The serializers run inside the call, key then value, on the record's
   * key and value (null included) with its topic and headers; one returning
   * a Promise is awaited before the hand-over, so sends that are not awaited
   * one by one may then reach the producer out of call order (`await
   * send()` keeps it).
   *
   * The client reads a `Buffer` or `Uint8Array` key or value, and what a
   * serializer returns, after `send()` returns. Don't write to it, transfer it or resize it until the Promise
   * from `send()` settles; awaiting `send()` is enough. A serializer must
   * return a new Buffer for each record. If other workers share the Buffer's
   * memory, they mustn't write to it until then either. *(deviation: Java
   * copies the bytes inside `send()`. Why: it saves a memory copy of each
   * Buffer, as the Python binding does.)* Header values are copied during
   * the call. Without a serializer, a string key or value is encoded to UTF-8 as Java's
   * `StringSerializer` encodes it (a lone surrogate as `?`). The record's
   * headers become read-only once its fields are checked: a later `add` or
   * `remove` throws `IllegalStateError` "RecordHeaders has been closed.".
   * *(deviation: Java's `send()` seals them only after its metadata wait;
   * here they are sealed before it. Why: the headers cross the C ABI during
   * `send()`, before the core's metadata wait. Goes when: the core seals the
   * headers itself, after that wait.)*
   *
   * Every failure rejects the returned Promise, an argument check's and the
   * closed producer's included: `send()` never throws. *(deviation: on a
   * closed producer the record is checked and converted before the core's
   * closed error, where Java checks the closed producer before it
   * serializes. Why: the binding keeps no state, and the conversion runs in
   * JavaScript before the call reaches the core.)*
   *
   * The record is typed `ProducerRecord<K | null, V | null>`: a record built
   * with a `null` key or value is a `ProducerRecord<null, ...>`, as
   * `ProducerRecord` has one signature per Java constructor, which binds a
   * `null` argument to `null` rather than `never`.
   *
   * The record is typed `ProducerRecord<K | null, V | null>`: a record built
   * with a `null` key or value is a `ProducerRecord<null, ...>`, as
   * `ProducerRecord` has one signature per Java constructor, which binds a
   * `null` argument to `null` rather than `never`.
   *
   * Inside a callback, do not call `flush()` or `close()` of the same
   * producer, run a transaction call, or await the future of a record sent
   * later: each waits for the callback itself. Before the callback's first
   * `await` the producer refuses them; after it they can hang. A callback
   * Promise that never settles has no time limit: it holds its record's
   * future, `flush()` and `close()`, `close({ timeout })` included.
   */
  send(options: {
    /** The record to send. */
    record: ProducerRecord<K | null, V | null>;
    /**
     * A user-supplied callback to execute when the record has been
     * acknowledged by the server (null indicates no callback).
     */
    callback: Callback | null;
  }): Promise<KafkaFuture<RecordMetadata>>;
  /**
   * Asynchronously send a record to a topic, Java's `send(record)`: the same
   * as `send({ record, callback: null })`.
   *
   * @param options - the record
   * @returns resolving at hand-over to the future of the record's metadata,
   *   rejecting with the error Java's send() would throw
   */
  send(options: {
    /** The record to send. */
    record: ProducerRecord<K | null, V | null>;
    callback?: never;
  }): Promise<KafkaFuture<RecordMetadata>>;
  /**
   * Asynchronously send a record to a topic: the record on its own, the same
   * as `send({record})`, Java's `send(record)`.
   *
   * @param record - The record to send
   * @returns resolving at hand-over to the future of the record's metadata,
   *   rejecting with the error Java's send() would throw
   */
  send(record: ProducerRecord<K | null, V | null>): Promise<KafkaFuture<RecordMetadata>>;
  send(options: unknown): Promise<KafkaFuture<RecordMetadata>> {
    try {
      return this.#send(options, arguments.length);
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the checks threw: an Error of the client's or JavaScript's (typed unknown by catch)
      return Promise.reject(error);
    }
  }

  // send()'s body: a throw is the returned Promise's rejection. The form,
  // then the arguments; a closed producer is the core's to report, once the
  // record reached it (javascript/CLAUDE.md, rule 1).
  #send(options: unknown, count: number): Promise<KafkaFuture<RecordMetadata>> {
    SEND_FORMS.match(options, count);
    const record = SEND_FORMS.value(options, 'record');
    // Java's doSend dereferences the record: a NullPointerException.
    if (record === null) throw new TypeError('record must not be null');
    if (!(record instanceof ProducerRecord)) throw new TypeError('record must be a ProducerRecord');
    const userCallback = checkCallback(SEND_FORMS.value(options, 'callback'));
    const keySerializer = this.#keySerializer;
    const valueSerializer = this.#valueSerializer;
    if (keySerializer === null && valueSerializer === null) return this.#handOver(record, userCallback, undefined, undefined);
    // Java's doSend serializes the key, then the value, with the record's
    // topic and headers (which a serializer may change: they are sealed only
    // at the hand-over). A serializer that returns a plain value costs no
    // microtask; one that returns a thenable is awaited.
    let key: unknown;
    if (keySerializer !== null) {
      key = keySerializer.serialize({ topic: record.topic, headers: record.headers, data: record.key });
      if (isThenable(key)) return this.#sendAwaiting(record, userCallback, key, NOT_SERIALIZED);
      key = serializedBytes(key, 'keySerializer');
    }
    let value: unknown;
    if (valueSerializer !== null) {
      value = valueSerializer.serialize({ topic: record.topic, headers: record.headers, data: record.value });
      if (isThenable(value)) return this.#sendAwaiting(record, userCallback, key, value);
      value = serializedBytes(value, 'valueSerializer');
    }
    return this.#handOver(record, userCallback, key as Uint8Array | null | undefined, value as Uint8Array | null | undefined);
  }

  // The rest of send() once a serializer returned a thenable: it awaits the
  // key's, runs the value serializer if it has not run yet and awaits its
  // result, then hands the record over (a producer closed in the meantime is
  // the core's to report). `key` and `value` are a thenable, bytes, or
  // undefined without a serializer; `value` is NOT_SERIALIZED when the value
  // serializer has not run yet.
  async #sendAwaiting(
    record: ProducerRecord<unknown, unknown>,
    userCallback: Callback | null,
    key: unknown,
    value: unknown,
  ): Promise<KafkaFuture<RecordMetadata>> {
    const keyBytes = isThenable(key) ? serializedBytes(await key, 'keySerializer') : (key as Uint8Array | null | undefined);
    let valueBytes: Uint8Array | null | undefined;
    if (value === NOT_SERIALIZED) {
      const valueSerializer = this.#valueSerializer;
      if (valueSerializer !== null) {
        const out: unknown = valueSerializer.serialize({ topic: record.topic, headers: record.headers, data: record.value });
        valueBytes = serializedBytes(isThenable(out) ? await out : out, 'valueSerializer');
      }
    } else if (isThenable(value)) {
      valueBytes = serializedBytes(await value, 'valueSerializer');
    } else {
      valueBytes = value as Uint8Array | null | undefined;
    }
    return this.#handOver(record, userCallback, keyBytes, valueBytes);
  }

  // The hand-over of a record: its key and value bytes from the serializers,
  // or undefined to send the record's own.
  #handOver(
    record: ProducerRecord<unknown, unknown>,
    userCallback: Callback | null,
    key: Uint8Array | null | undefined,
    value: Uint8Array | null | undefined,
  ): Promise<KafkaFuture<RecordMetadata>> {
    if (this.#pumpNext || this.#fastPathSends % PUMP_EVERY === 0) {
      this.#pumpNext = false;
      // A no-op inside a drain (a send from a delivery callback).
      debug('KafkaProducer pump');
      native.producerPump(this.#handle);
    }
    const args = record.internalToNative(key, value);
    const future: KafkaFuture<RecordMetadata> = KafkaFuture.internalCreate(true);
    let resolve!: (future: KafkaFuture<RecordMetadata>) => void;
    let reject!: (error: unknown) => void;
    const accepted = new Promise<KafkaFuture<RecordMetadata>>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    // The hand-over, inside producerSend for a record taken or refused at once,
    // later otherwise: an error is the one Java's send() would throw. Never
    // marked handled, so an un-awaited send that fails surfaces.
    let inCall = true;
    let handedOver = false;
    const settle = (error: unknown): void => {
      if (error !== null) reject(error);
      else resolve(future);
    };
    const onHandover = (error: unknown, nativeFuture: number | null): void => {
      handedOver = true;
      if (nativeFuture !== null) future.internalSetNative(this.#handle, nativeFuture);
      if (!inCall) {
        // Queued. Its completion may run in another send()'s pump, with no
        // event-loop turn since the count started: the count stays, and while
        // a deferral turn is pending it settles in that turn too, after the
        // sends already deferred (an await on it would otherwise resume in a
        // microtask, with no turn).
        if (this.#deferred !== null) this.#deferSettlement(resolve, reject, future, error);
        else settle(error);
      } else if (this.#fastPathSends++ >= FAST_PATH_YIELD_EVERY || this.#deferred !== null) {
        // While a deferral turn is pending, a send taken at once settles in
        // that turn too, in order.
        this.#deferSettlement(resolve, reject, future, error);
      } else {
        settle(error);
      }
    };
    // The record's delivery callback, run synchronously inside the native
    // drain: the user callback first (the only user code the drain runs),
    // then the delivery recorded on the future, which completes it, before the
    // core has the report this returns for (javascript/CLAUDE.md, rules 5 and
    // 6): its get() Promises settle as microtasks after the drain.
    // A message returned is what the callback threw, for the core to log; the
    // callback id the core reports by stays in C (native/producer.c,
    // send_on_completion). A callback that returned a thenable is awaited:
    // the delivery is recorded, and the Promise returned here settles, once
    // the thenable did, and the core's report waits for that
    // (native/producer.c, send_defer). A callback returning nothing allocates
    // no Promise.
    const onComplete = (metadata: RecordMetadata, error: Error | null): string | undefined | Promise<string | undefined> => {
      let failure: string | undefined;
      let returned: unknown;
      if (userCallback !== null) {
        try {
          returned = userCallback({ metadata, exception: error });
          // Java's void: reported at once, with no Promise (the common case).
          // Any other result is Promise.resolve(result), reported once it
          // settled (root CLAUDE.md, JavaScript Binding Conventions, Threads
          // and callbacks).
          if (returned !== undefined) return completeWhenSettled(returned, future, metadata, error);
        } catch (thrown) {
          failure = thrownMessage(thrown);
        }
      }
      return completeRecord(future, metadata, error, failure);
    };
    try {
      native.producerSend(this.#handle, ...args, onHandover, onComplete);
    } finally {
      inCall = false;
    }
    // After a send that had to wait, the next send runs the queued callbacks
    // first (Python's AsyncProducer.send). Python also restarts its count
    // there, because its await turns the asyncio loop; a Promise resumes in a
    // microtask instead, so the count stays until the event loop turns.
    if (!handedOver) this.#pumpNext = true;
    return accepted;
  }

  // Settles a send's Promise once the event loop has turned: one setImmediate
  // per turn settles every deferred send in order and restarts the count.
  // Only the resolving function and its value are kept, not a closure over
  // the send() context, so a delivered record's context can be collected
  // while its settlement waits.
  #deferSettlement(
    resolve: (future: KafkaFuture<RecordMetadata>) => void,
    reject: (error: unknown) => void,
    future: KafkaFuture<RecordMetadata>,
    error: unknown,
  ): void {
    if (this.#deferred === null) {
      this.#deferred = [];
      setImmediate(() => {
        const deferred = this.#deferred as unknown[];
        this.#deferred = null;
        this.#fastPathSends = 0;
        for (let i = 0; i < deferred.length; i += 2) (deferred[i] as (value: unknown) => void)(deferred[i + 1]);
      });
    }
    if (error !== null) this.#deferred.push(reject, error);
    else this.#deferred.push(resolve, future);
  }

  /**
   * Invoking this method makes all buffered records immediately available to
   * send and resolves once the requests associated with these records
   * complete and their callbacks ran. The records are those whose `send()`
   * had resolved before `flush()` was called: their futures are all done when
   * it resolves.
   *
   * Calling it from inside a callback rejects, as Java's flush() throws there,
   * until the callback's first `await`; after it, it waits for that callback,
   * which waits for it: do not call it from a callback.
   *
   * @throws `IllegalArgumentError` rejects with it when it is given an
   *   argument: Java's flush() takes none
   * @throws `TypeError` rejects with it when it is given `undefined`
   * @throws `InterruptError` rejects with it if the thread is interrupted while
   *   blocked
   * @throws `KafkaError` rejects with it if the method is invoked inside a
   *   `send` callback
   *
   * @remarks A callback that returned a Promise counts as run once it
   * settled, so a callback's Promise must not wait for this flush. On a
   * closed producer it does what the core does.
   */
  flush(): Promise<void>;
  flush(options?: unknown): Promise<void> {
    try {
      // Java's flush() takes nothing: an argument (1.x's flush(timeout,
      // callback)) is refused, never dropped, and so is flush(undefined).
      FLUSH_FORMS.match(options, arguments.length);
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the check threw: an Error of the client's (typed unknown by catch)
      return Promise.reject(error);
    }
    return new Promise<void>((resolve, reject) => {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the error the core reported, a client Error src/internal/errors.ts built (typed unknown at the native boundary)
      native.producerFlush(this.#handle, (error) => (error !== null ? reject(error) : resolve()));
    });
  }

  /**
   * Get the partition metadata for the given topic. This can be used for
   * custom partitioning.
   *
   * This will attempt to refresh metadata until it finds the topic in it, or
   * the configured `max.block.ms` expires.
   *
   * @param options - the topic
   * @returns resolving to the topic's partitions, a new array
   * @throws `AuthenticationError` rejects with it if authentication fails.
   *   See the error for more details
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   specified topic. See the error for more details
   * @throws `InterruptError` rejects with it if the thread is interrupted
   *   while blocked
   * @throws `TimeoutError` rejects with it if the topic cannot be found in
   *   metadata within `max.block.ms`
   * @throws `BootstrapResolutionError` rejects with it if DNS resolution of
   *   the bootstrap servers fails within `bootstrap.resolve.timeout.ms`
   * @throws `KafkaError` rejects with it for all Kafka-related errors,
   *   including the case where this method is called after producer close
   * @throws `IllegalArgumentError` rejects with it when the given parameters
   *   are not `{topic}`
   * @throws `TypeError` rejects with it when `topic` is null ("topic cannot be
   *   null", the core's), `undefined` or not a string
   *
   * @remarks Java's blocking call returns a Promise here, and Java's `List`
   * an array.
   */
  partitionsFor(options: {
    /** The topic to get partition metadata for. */
    topic: string;
  }): Promise<PartitionInfo[]>;
  /**
   * Get the partition metadata for the given topic: the topic on its own,
   * the same as `partitionsFor({topic})`.
   *
   * @param topic - The topic to get partition metadata for
   * @returns resolving to the topic's partitions
   */
  partitionsFor(topic: string): Promise<PartitionInfo[]>;
  partitionsFor(options: unknown): Promise<PartitionInfo[]> {
    let topic: unknown;
    try {
      PARTITIONS_FOR_FORMS.match(options, arguments.length);
      topic = PARTITIONS_FOR_FORMS.value(options, 'topic');
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the checks threw: an Error of the client's or JavaScript's (typed unknown by catch)
      return Promise.reject(error);
    }
    // A topic that is not a string is the addon's TypeError, thrown here, so
    // it rejects the Promise.
    return new Promise<PartitionInfo[]>((resolve, reject) => {
      native.producerPartitionsFor(this.#handle, topic as string | null, (error, partitions) => {
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the error the core reported, a client Error src/internal/errors.ts built (typed unknown at the native boundary)
        if (error !== null) return reject(error);
        try {
          resolve(
            (partitions ?? []).map(
              (fields) =>
                new PartitionInfo({
                  topic: fields.topic,
                  partition: fields.partition,
                  leader: nodeOf(fields.leader),
                  replicas: fields.replicas === null ? null : fields.replicas.map((n) => nodeOf(n) as Node),
                  inSyncReplicas: fields.inSyncReplicas === null ? null : fields.inSyncReplicas.map((n) => nodeOf(n) as Node),
                  offlineReplicas: fields.offlineReplicas === null ? null : fields.offlineReplicas.map((n) => nodeOf(n) as Node),
                }),
            ),
          );
        } catch (thrown) {
          // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what building the values threw (typed unknown by catch)
          reject(thrown);
        }
      });
    });
  }

  /**
   * Close this producer. This method blocks until all previously sent
   * requests complete: Java's `close()`, which waits as
   * `close(Duration.ofMillis(Long.MAX_VALUE))` does, with no time limit.
   *
   * @throws `InterruptError` rejects with it if the thread is interrupted while
   *   blocked
   * @throws `KafkaError` rejects with it if an unexpected error occurs while
   *   trying to close the client; this error should be treated as fatal and
   *   indicate the client is no longer usable
   * @throws `TypeError` rejects with it when it is given `undefined`
   *
   * @remarks The records waited for are those whose `send()` had resolved
   * before `close()` was called, and their callbacks' Promises: a callback
   * Promise that never settles holds the close. Calling it again closes as
   * the core does, harmlessly. Every failure rejects the returned Promise:
   * `close()` never throws.
   *
   * Close every producer: a producer keeps Node running until it is closed,
   * and no garbage collection closes one, so a script that never calls
   * `close()` never exits on its own. Once closed, the producer no longer
   * holds Node, and its native client is released when the producer is
   * garbage collected. `await using` closes a producer at the end of its
   * scope (`[Symbol.asyncDispose]`). Do not call it from a send callback: it
   * waits for that callback.
   */
  close(): Promise<void>;
  /**
   * This method waits up to `timeout` for the producer to complete the
   * sending of all incomplete requests. If the producer is unable to complete
   * all requests before the timeout expires, this method will fail any unsent
   * and unacknowledged records immediately. A timeout of zero means do not
   * wait for pending send requests to complete.
   *
   * @param options - the timeout
   * @throws `IllegalArgumentError` rejects with it if the `timeout` is
   *   negative, or if the given parameters are not `{}` or `{timeout}`, or a
   *   bare value is neither a number nor a `Temporal.Duration`
   * @throws `TypeError` rejects with it when `timeout` is null ("timeout must
   *   not be null"), `undefined`, or neither a number nor a
   *   `Temporal.Duration`
   * @throws `RangeError` rejects with it when `timeout` is `NaN`,
   *   `-Infinity`, holds years, months or weeks, or is above
   *   `Number.MAX_SAFE_INTEGER` milliseconds
   * @throws `InterruptError` rejects with it if the thread is interrupted while
   *   blocked
   * @throws `KafkaError` rejects with it if an unexpected error occurs while
   *   trying to close the client; this error should be treated as fatal and
   *   indicate the client is no longer usable
   *
   * @remarks A negative `timeout` is rejected by the client, with Java's
   * error, and changes nothing. A callback Promise that never settles holds
   * the close past its timeout.
   */
  close(options: CloseArgs): Promise<void>;
  /**
   * Close this producer, waiting up to `timeout` for the pending requests:
   * the timeout on its own, the same as `close({timeout})`, Java's
   * close(Duration).
   *
   * @param timeout - The maximum time to wait for the producer to complete
   *   any pending requests. A timeout of zero means do not wait.
   */
  close(timeout: Duration): Promise<void>;
  close(options?: unknown): Promise<void> {
    let form: number;
    try {
      form = CLOSE_FORMS.match(options, arguments.length);
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the check threw: an Error of the client's or JavaScript's (typed unknown by catch)
      return Promise.reject(error);
    }
    // The form decides the FFI call: null for Java's close(), the timeout for
    // close(Duration). A negative timeout goes to the FFI as it is, which
    // rejects it with Java's error and closes nothing (root CLAUDE.md,
    // JavaScript Binding Conventions, Signatures).
    let timeoutMs: number | bigint | null;
    try {
      timeoutMs = form === 0 ? null : durationToMillis(CLOSE_FORMS.value(options, 'timeout'), 'timeout');
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the checks threw: an Error of the client's or JavaScript's (typed unknown by catch)
      return Promise.reject(error);
    }
    debug('KafkaProducer closing, timeout %s', form === 0 ? 'none' : String(timeoutMs));
    return new Promise<void>((resolve, reject) => {
      native.producerClose(this.#handle, timeoutMs, (error, closedNow) => {
        // This close closed the producer: the core reported it closed.
        if (closedNow) {
          producerClosed();
          // Java's close closes the serializers last, before it returns.
          this.#closeSerializers();
          debug('KafkaProducer closed');
        }
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the error the core reported, a client Error src/internal/errors.ts built (typed unknown at the native boundary)
        if (error !== null) reject(error);
        else resolve();
      });
    });
  }

  /**
   * Closes this producer as `close()` does, so that a producer declared with
   * `await using` is closed when its scope ends.
   *
   * @returns resolving once the producer is closed, rejecting as `close()`
   *   rejects
   *
   * @remarks *(js-only)* JavaScript's async disposal protocol, the form Java's
   * `AutoCloseable.close()` takes in a `try`-with-resources block.
   */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }
}
