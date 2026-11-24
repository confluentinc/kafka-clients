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
// producer is given run here, in JavaScript, inside send() (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Serialization).

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('../internal/native');
import { JavaForms } from '../internal/args';
import { checkConfigs, configEntries } from '../internal/config';
import { debug } from '../internal/debug';
import { durationToMillis, isDurationOrNull } from '../internal/duration';
import { warnPreview } from '../internal/lifecycle';
import { checkSerializer, closeSerializer, configuredSerializer, isThenable, serializedBytes, serializerFailure } from '../internal/serde';
import { ApiError } from '../common/errors/api_error';
import { KafkaError } from '../common/kafka_error';
import { KafkaFuture } from '../common/kafka_future';
import type { Metric } from '../common/metric';
import { MetricName } from '../common/metric_name';
import { MetricNameMap } from '../common/metric_name_map';
import { KafkaMetric } from '../common/metrics/kafka_metric';
import { Node } from '../common/node';
import { PartitionInfo } from '../common/partition_info';
import { TopicPartition } from '../common/topic_partition';
import type { Serializer } from '../common/serialization/serializer';
import type { Duration } from '../duration';
import type { Callback } from './callback';
import type { Producer } from './producer';
import type { ProducerConfigs } from './producer_configs';
import { ProducerRecord } from './producer_record';
import { RecordMetadata } from './record_metadata';

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

/**
 * The serde config keys of a producer's configuration, typed by the
 * serializers their functions return, so that a producer built through them
 * infers its type arguments as one built with serializer arguments does.
 */
type SerdeConfigs<K, V> = {
  'key.serializer'?: ((options: { configs: Readonly<Record<string, unknown>>; isKey: boolean }) => Serializer<K>) | null;
  'value.serializer'?: ((options: { configs: Readonly<Record<string, unknown>>; isKey: boolean }) => Serializer<V>) | null;
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
const METRICS_FORMS = new JavaForms('metrics', [[]], { reader: true });
const PARTITIONS_FOR_FORMS = new JavaForms('partitionsFor', [['topic']], {
  bareTypes: { topic: (value) => value === null || typeof value === 'string' },
});

/**
 * The native layer's node fields as a Node, Java's five-parameter constructor.
 *
 * @param fields - the node's fields, or null
 * @returns the node, or null
 */
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

/**
 * Whether `error` is the construction failure Java's constructor throws from its catch.
 *
 * @param error - the error thrown
 * @returns whether it is the construction failure
 */
function isConstructionFailure(error: unknown): boolean {
  return error instanceof KafkaError && error.constructor === KafkaError && error.message === CONSTRUCTION_FAILED;
}

/**
 * The callback `send` was given, or null; throws a TypeError for a non-function.
 *
 * @param callback - the callback given
 * @returns the callback, or null
 */
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
 *
 * @param future - the record's future
 * @param metadata - the record's metadata
 * @param error - the delivery's error, or null
 * @param failure - the text of what the callback threw, or undefined
 * @returns `failure`
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
 *
 * @param returned - what the callback returned
 * @param future - the record's future
 * @param metadata - the record's metadata
 * @param error - the delivery's error, or null
 * @returns a Promise of the text of the rejection, or of undefined
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
 *
 * @param thrown - the thrown value
 * @returns its text
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
 * Java's metadata of a failed record, `RecordMetadata(topicPartition, -1, -1,
 * NO_TIMESTAMP, -1, -1)`, as the core gives it for a record not partitioned
 * yet: the record's topic, and its partition or -1.
 *
 * @param topic - the record's topic
 * @param partition - the record's partition, or null
 * @returns the metadata
 */
function failedMetadata(topic: string, partition: number | null): RecordMetadata {
  return new RecordMetadata({
    topicPartition: new TopicPartition({ topic, partition: partition ?? RecordMetadata.UNKNOWN_PARTITION }),
    baseOffset: -1,
    batchIndex: -1,
    timestamp: -1,
    serializedKeySize: -1,
    serializedValueSize: -1,
  });
}

/**
 * `KafkaProducer#doSend`'s answer to a serializer's throw, once
 * `serializerFailure` made it: an `ApiError` (Java's `catch (ApiException
 * e)`) runs the callback once, on the calling thread, with Java's failure
 * metadata, `RecordMetadata(appendCallbacks.topicPartition(), -1, -1,
 * NO_TIMESTAMP, -1, -1)` (the record's topic and its partition, or -1 as it
 * is not partitioned yet), and resolves to a done future failed with it,
 * Java's `FutureFailure`; any other error is the rejection, as Java rethrows
 * it. What the callback throws is the rejection, as Java's onCompletion
 * throws out of `doSend`; a Promise it returns is awaited first, and its
 * rejection is the rejection too.
 *
 * @param record - the record
 * @param userCallback - the record's callback, or null
 * @param error - the error to answer with
 * @returns resolving to the failed future, or rejecting with `error`
 */
function serializerFailed(
  record: ProducerRecord<unknown, unknown>,
  userCallback: Callback | null,
  error: unknown,
): Promise<KafkaFuture<RecordMetadata>> {
  // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the serializer threw, whatever it is, as Java rethrows it (typed unknown by catch)
  if (!(error instanceof ApiError)) return Promise.reject(error);
  const future: KafkaFuture<RecordMetadata> = KafkaFuture.internalCreate(true);
  const failed = (): KafkaFuture<RecordMetadata> => {
    future.internalComplete(null as unknown as RecordMetadata, error);
    return future;
  };
  if (userCallback === null) return Promise.resolve(failed());
  const metadata = failedMetadata(record.topic, record.partition);
  let returned: unknown;
  try {
    returned = userCallback({ metadata, exception: error });
  } catch (thrown) {
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the user's callback threw, whatever it is (typed unknown by catch)
    return Promise.reject(thrown);
  }
  if (returned === undefined) return Promise.resolve(failed());
  return Promise.resolve(returned).then(failed);
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
  // callbacks first (JS conventions, Threads and callbacks). It leaves the
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
   *   the core's, with Java's message naming the key; the client's own for
   *   a `key.serializer` or `value.serializer` that is no serializer
   *   function) or the producer cannot be built ("Failed to construct kafka
   *   producer", also when a serializer function throws)
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{configs}` or `{configs, keySerializer, valueSerializer}`, the configs
   *   are not an object, or a serializer is a function that makes one
   *   ("did you forget '()'?")
   * @throws `TypeError` when `configs` is null, `undefined` or not a plain
   *   object (a `Map` included), a config key or value holds a null byte,
   *   which the C ABI would read as the end of the string, or a serializer is
   *   neither null nor an object with a `serialize` method
   *
   * @remarks Java's `Map` and `Properties` forms are `configs`.
   * `key.serializer` and `value.serializer` take a function
   * `({ configs, isKey }) => serializer` *(deviation: Java takes a class)*.
   * These keys, and any non-producer key the function reads, don't reach the
   * core. *(deviation)* The function gets the configuration as given, without
   * Java's `client.id`, before the core checks it. Without serializers, keys
   * and values are sent as bytes *(deviation)*. Once per process it emits an
   * `ExperimentalWarning`.
   */
  constructor(options: {
    /** The producer configs: a plain object of Java's ProducerConfig keys. */
    configs: ProducerConfigs & SerdeConfigs<K, V>;
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
   * over. A null serializer is the one its config key makes, as in Java, or,
   * with no such key, sends the record's bytes as they are, binding
   * `Uint8Array`; a serializer given wins, and its config key is then
   * ignored. The producer closes both serializers when it closes.
   */
  constructor(options: {
    /** The producer configs: a plain object of Java's ProducerConfig keys. */
    configs: ProducerConfigs & SerdeConfigs<K, V>;
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
  constructor(configs: ProducerConfigs & SerdeConfigs<K, V>);
  constructor(options: unknown) {
    const form = CONSTRUCTOR_FORMS.match(options, arguments.length);
    const configs = checkConfigs(CONSTRUCTOR_FORMS.value(options, 'configs'));
    const keyArgument = form === 0 ? null : checkSerializer(CONSTRUCTOR_FORMS.value(options, 'keySerializer'), 'keySerializer');
    const valueArgument = form === 0 ? null : checkSerializer(CONSTRUCTOR_FORMS.value(options, 'valueSerializer'), 'valueSerializer');
    // A serializer given wins, and its config key is ignored; a null one (or
    // none) is the config key's, as Java's `if (keySerializer == null)`. The
    // keys the config functions read stay out of the core's configuration.
    const read = new Set<string>();
    let keyConfigured: Serializer<unknown> | null = null;
    let valueConfigured: Serializer<unknown> | null = null;
    // How many serializers construction has taken, key first: Java's
    // constructor assigns the key serializer's plugin, then the value's, and
    // its catch closes only those already assigned.
    let taken = 0;
    try {
      if (keyArgument === null) keyConfigured = configuredSerializer(configs, 'key.serializer', read);
      taken = 1;
      if (valueArgument === null) valueConfigured = configuredSerializer(configs, 'value.serializer', read);
      taken = 2;
      this.#keySerializer = keyArgument ?? keyConfigured;
      this.#valueSerializer = valueArgument ?? valueConfigured;
      this.#handle = native.producerCreate(configEntries(configs, read));
    } catch (error) {
      // Java's constructor closes the serializers it has taken in the catch
      // around its body, whose failure it relabels "Failed to construct kafka
      // producer" (the core's KafkaError, or the config function's failure).
      // A ConfigError of the configuration's check is thrown before that
      // catch: the serializers given stay open, as in Java. Those the config
      // functions made are the producer's own, and nobody else can close
      // them.
      const construction = isConstructionFailure(error);
      if (taken >= 1) closeSerializer(construction ? (keyArgument ?? keyConfigured) : keyConfigured, 'keySerializer');
      if (taken >= 2) closeSerializer(construction ? (valueArgument ?? valueConfigured) : valueConfigured, 'valueSerializer');
      throw error;
    }
    warnPreview();
    debug('KafkaProducer created');
  }

  // Java's close closes both serializers; a failure is a process warning.
  #closeSerializers(): void {
    closeSerializer(this.#keySerializer, 'keySerializer');
    closeSerializer(this.#valueSerializer, 'valueSerializer');
  }

  send(options: {
    /** The record to send. */
    record: ProducerRecord<K, V>;
    /**
     * A user-supplied callback to execute when the record has been
     * acknowledged by the server (null indicates no callback).
     */
    callback: Callback | null;
  }): Promise<KafkaFuture<RecordMetadata>>;
  send(options: {
    /** The record to send. */
    record: ProducerRecord<K, V>;
    callback?: never;
  }): Promise<KafkaFuture<RecordMetadata>>;
  send(record: ProducerRecord<K, V>): Promise<KafkaFuture<RecordMetadata>>;
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
    // A serializer's throw is answered as Java's doSend answers it
    // (serializerFailed).
    let key: unknown;
    if (keySerializer !== null) {
      try {
        key = keySerializer.serialize({ topic: record.topic, headers: record.headers, data: record.key });
      } catch (thrown) {
        return serializerFailed(record, userCallback, serializerFailure(thrown, true, record.key, keySerializer));
      }
      if (isThenable(key)) return this.#sendAwaiting(record, userCallback, key, NOT_SERIALIZED);
      key = serializedBytes(key, 'keySerializer');
    }
    let value: unknown;
    if (valueSerializer !== null) {
      try {
        value = valueSerializer.serialize({ topic: record.topic, headers: record.headers, data: record.value });
      } catch (thrown) {
        return serializerFailed(record, userCallback, serializerFailure(thrown, false, record.value, valueSerializer));
      }
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
    // What a serializer throws or rejects with is answered as Java's doSend
    // answers it (serializerFailed).
    let keyBytes: Uint8Array | null | undefined;
    if (isThenable(key)) {
      let out: unknown;
      try {
        out = await key;
      } catch (thrown) {
        return serializerFailed(record, userCallback, serializerFailure(thrown, true, record.key, this.#keySerializer as object));
      }
      keyBytes = serializedBytes(out, 'keySerializer');
    } else {
      keyBytes = key as Uint8Array | null | undefined;
    }
    let pending: unknown = value;
    if (value === NOT_SERIALIZED) {
      pending = undefined;
      const valueSerializer = this.#valueSerializer;
      if (valueSerializer !== null) {
        try {
          pending = valueSerializer.serialize({ topic: record.topic, headers: record.headers, data: record.value });
        } catch (thrown) {
          return serializerFailed(record, userCallback, serializerFailure(thrown, false, record.value, valueSerializer));
        }
        if (!isThenable(pending)) pending = serializedBytes(pending, 'valueSerializer');
      }
    }
    let valueBytes: Uint8Array | null | undefined;
    if (isThenable(pending)) {
      let out: unknown;
      try {
        out = await pending;
      } catch (thrown) {
        return serializerFailed(record, userCallback, serializerFailure(thrown, false, record.value, this.#valueSerializer as object));
      }
      valueBytes = serializedBytes(out, 'valueSerializer');
    } else {
      valueBytes = pending as Uint8Array | null | undefined;
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
    // no Promise. The addon calls this in the callback's turn: one callback at
    // a time, in completion order (native/producer.c, send_on_completion).
    // Metadata the addon could not build is null, its error the failure: the
    // callback gets Java's metadata of a failed record. Only the topic and
    // partition are kept for it: the record, and its Buffers, go at the
    // hand-over.
    const { topic, partition } = record;
    const onComplete = (built: RecordMetadata | null, error: Error | null): string | undefined | Promise<string | undefined> => {
      const metadata = built ?? failedMetadata(topic, partition);
      let failure: string | undefined;
      let returned: unknown;
      if (userCallback !== null) {
        try {
          returned = userCallback({ metadata, exception: error });
          // Java's void: reported at once, with no Promise (the common case).
          // Any other result is Promise.resolve(result), reported once it
          // settled (javascript/CLAUDE.md, JavaScript Binding Conventions, Threads
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

  partitionsFor(options: {
    /** The topic to get partition metadata for. */
    topic: string;
  }): Promise<PartitionInfo[]>;
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

  metrics(): MetricNameMap<Metric> {
    METRICS_FORMS.match(undefined, arguments.length);
    const entries: Array<[MetricName, Metric]> = native.producerMetrics(this.#handle).map((m) => {
      const name = new MetricName({ name: m.name, group: m.group, description: m.description, tags: new Map(m.tags) });
      return [name, KafkaMetric.internalSnapshot(name, m.value, m.measurable)];
    });
    return new MetricNameMap(entries);
  }

  close(): Promise<void>;
  close(options: CloseArgs): Promise<void>;
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
    // rejects it with Java's error and closes nothing (javascript/CLAUDE.md,
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

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }
}
