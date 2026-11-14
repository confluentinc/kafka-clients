// Copyright 2026 Confluent Inc.
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

// Java's MockProducer over the C FFI's MockProducer (root CLAUDE.md,
// JavaScript Binding Conventions, Implementation over the FFI, Q62): the mock
// is the core's, reached through the addon as KafkaProducer is, and the
// records it keeps are the app's own: each key and value crosses as the FFI's
// `void *`, which the FFI's mock keeps without reading, and the addon holds
// the record until clear() or the mock is destroyed (native/producer.c,
// MockRecord). The serializers the mock is given run here, on every send, as
// Java's mock runs them.
//
// Left out (Scope, Q30): Java's MockProducer(Cluster, boolean, Partitioner,
// Serializer, Serializer), whose only extra parameter is the untranslated
// Cluster, so the mock's cluster is always Java's Cluster.empty(). Left out by
// request: the transaction calls (initTransactions, beginTransaction,
// sendOffsetsToTransaction, commitTransaction, abortTransaction,
// fenceProducer), their state (transactionInitialized, transactionInFlight,
// transactionCommitted, transactionAborted, sentOffsets, commitCount,
// uncommittedRecords, consumerGroupOffsetsHistory, uncommittedOffsets) and
// their error fields. Left out for want of an FFI entry point or a native
// value the binding can build: setMockMetrics (the FFI takes a native
// KafkaMetric_t, which the binding cannot build), clientInstanceId,
// disableTelemetry, injectTimeoutException, setClientInstanceId,
// registerMetricForSubscription, unregisterMetricFromSubscription and
// addedMetrics.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('../internal/native');
import { JavaForms } from '../internal/args';
import { durationToMillis, isDurationOrNull } from '../internal/duration';
import { checkSerializer, isThenable } from '../internal/serde';
import { KafkaFuture } from '../common/kafka_future';
import type { Metric } from '../common/metric';
import { MetricName } from '../common/metric_name';
import { MetricNameMap } from '../common/metric_name_map';
import { KafkaMetric } from '../common/metrics/kafka_metric';
import { Node } from '../common/node';
import { PartitionInfo } from '../common/partition_info';
import { Headers } from '../common/header/headers';
import type { Serializer } from '../common/serialization/serializer';
import type { Duration } from '../duration';
import type { Callback } from './callback';
import type { CloseArgs } from './kafka_producer';
import type { Partitioner } from './partitioner';
import type { Producer } from './producer';
import { ProducerRecord } from './producer_record';
import type { RecordMetadata } from './record_metadata';

/** The native producer handle (`internal/native.ts`). */
type ProducerHandle = ReturnType<typeof native.mockProducerCreate>;

// Java's forms of each method, in Java declaration order (src/internal/args.ts).
const CONSTRUCTOR_FORMS = new JavaForms('MockProducer', [[], ['autoComplete', 'partitioner', 'keySerializer', 'valueSerializer']]);
const SEND_FORMS = new JavaForms('send', [['record'], ['record', 'callback']], {
  bareTypes: { record: (value) => value === null || value instanceof ProducerRecord },
});
const CLOSE_FORMS = new JavaForms('close', [[], ['timeout']], { bareTypes: { timeout: isDurationOrNull } });
const FLUSH_FORMS = new JavaForms('flush', [[]]);
const METRICS_FORMS = new JavaForms('metrics', [[]], { reader: true });
const PARTITIONS_FOR_FORMS = new JavaForms('partitionsFor', [['topic']], {
  bareTypes: { topic: (value) => value === null || typeof value === 'string' },
});
const HISTORY_FORMS = new JavaForms('history', [[]], { reader: true });
const CLEAR_FORMS = new JavaForms('clear', [[]]);
const COMPLETE_NEXT_FORMS = new JavaForms('completeNext', [[]]);
const isErrorOrNull = (value: unknown): boolean => value === null || value instanceof Error;
const ERROR_NEXT_FORMS = new JavaForms('errorNext', [['e']], { bareTypes: { e: isErrorOrNull } });
const CLOSED_FORMS = new JavaForms('closed', [[]], { reader: true });
const FLUSHED_FORMS = new JavaForms('flushed', [[]], { reader: true });
const SET_SEND_FORMS = new JavaForms('setSendException', [['sendException']], { bareTypes: { sendException: isErrorOrNull } });
const SET_FLUSH_FORMS = new JavaForms('setFlushException', [['flushException']], { bareTypes: { flushException: isErrorOrNull } });
const SET_PARTITIONS_FOR_FORMS = new JavaForms('setPartitionsForException', [['partitionsForException']], {
  bareTypes: { partitionsForException: isErrorOrNull },
});
const SET_CLOSE_FORMS = new JavaForms('setCloseException', [['closeException']], { bareTypes: { closeException: isErrorOrNull } });

// After this many sends in a row settled inside their call, the next one
// settles once the event loop has turned (KafkaProducer's
// FAST_PATH_YIELD_EVERY), so a loop awaiting the mock's sends lets timers
// and I/O run.
const FAST_PATH_YIELD_EVERY = 1000;

/** The native layer's node fields as a Node, Java's five-parameter constructor. */
function nodeOf(fields: { id: number; host: string | null; port: number; rack: string | null; isFenced: boolean } | null): Node | null {
  return fields === null ? null : new Node(fields);
}

/** The callback `send` was given, or null; throws a TypeError for a non-function. */
function checkCallback(callback: unknown): Callback | null {
  if (callback === undefined || callback === null) return null;
  if (typeof callback !== 'function') throw new TypeError('callback must be a function ({ metadata, exception }) => void');
  return callback as Callback;
}

/** An error to give the mock: an `Error`, or null; throws a TypeError for any other value. */
function checkError(error: unknown, name: string): Error | null {
  if (error === null || error instanceof Error) return error;
  throw new TypeError(`${name} must be an Error or null`);
}

/** Records a record's delivery on its future after its callback ran, and gives back what the callback threw. */
function completeRecord(
  future: KafkaFuture<RecordMetadata>,
  metadata: RecordMetadata,
  error: Error | null,
  failure: string | undefined,
): string | undefined {
  future.internalSetDelivery(metadata, error);
  return failure;
}

/** `Promise.resolve(returned)`, the callback's result, with the record's delivery recorded once it settled. */
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

/** The text of what a callback threw, which the core logs; it never throws. */
function thrownMessage(thrown: unknown): string {
  try {
    if (thrown instanceof Error) return `${String(thrown.name)}: ${String(thrown.message)}`;
    return String(thrown);
  } catch {
    return 'the callback threw a value that cannot be converted to a string';
  }
}

/**
 * A mock of the producer interface you can use for testing code that uses
 * Kafka.
 *
 * By default this mock will synchronously complete each send call
 * successfully. However it can be configured to allow the user to control
 * the completion of the call and supply an optional error for the producer to
 * throw.
 *
 * @remarks The mock is the client core's, reached as `KafkaProducer` is:
 * `send()`, `flush()`, `partitionsFor()` and `close()` return Promises, and
 * the delivery callbacks run on the JS thread, inside the mock call that
 * completes the record (`send()` with `autoComplete`, `completeNext()`,
 * `errorNext()`, `flush()`), before it returns. `history()` returns the app's
 * own records; the mock holds each until `clear()` or its native client is
 * released. Like a producer, a mock keeps Node running until `close()`
 * closes it, so close every mock. The mock's cluster is always empty: Java's
 * constructor taking a `Cluster` is left out (Cluster is not translated), so
 * the partitioner is never called, as in Java.
 */
export class MockProducer<K, V> implements Producer<K, V> {
  #handle: ProducerHandle;
  // The serializers given, or null (typed over unknown: send() reads the
  // record as ProducerRecord<unknown, unknown>).
  readonly #keySerializer: Serializer<unknown> | null;
  readonly #valueSerializer: Serializer<unknown> | null;
  // Sends settled inside their call since the event loop last turned.
  #fastPathSends = 0;
  // Settlements waiting for the event-loop turn, or null when none is
  // scheduled: each a settle function, then the value it settles with.
  #deferred: unknown[] | null = null;

  /**
   * Create a new mock producer with invented metadata.
   *
   * Equivalent to `new MockProducer({ autoComplete: false, partitioner:
   * null, keySerializer: null, valueSerializer: null })`.
   */
  constructor();
  /**
   * Create a new mock producer with invented metadata the given autoComplete
   * setting, partitioner and key\value serializers.
   *
   * @param options - the autoComplete setting, the partitioner and the
   *   serializers
   * @throws `IllegalArgumentError` when the given parameters are not `()` or
   *   `{autoComplete, partitioner, keySerializer, valueSerializer}`, or a
   *   serializer is a function that makes one ("did you forget '()'?")
   * @throws `TypeError` when `autoComplete` is not a boolean, or a serializer
   *   is neither null nor an object with a `serialize` method
   *
   * @remarks Java's form with a `Cluster` is left out, so the cluster is
   * always empty, as `Cluster.empty()`, and the partitioner is never
   * called, as in Java with an empty cluster. A serializer runs on every
   * send, with empty headers, and what it returns is not kept, as in Java; a
   * null one is not run.
   */
  constructor(options: {
    /**
     * If true automatically complete all requests successfully and execute
     * the callback. Otherwise the user must call `completeNext()` or
     * `errorNext(e)` after `send()` to complete the call and unblock the
     * future that is returned.
     */
    autoComplete: boolean;
    /** The partition strategy, or null. */
    // eslint-disable-next-line @typescript-eslint/no-redundant-type-constituents -- root CLAUDE.md, JavaScript Binding Conventions, Scope (Q30) types it Partitioner | null: Partitioner is a placeholder of unknown until it is translated
    partitioner: Partitioner | null;
    /** The serializer for key that implements `Serializer`, or null. */
    keySerializer: Serializer<K> | null;
    /** The serializer for value that implements `Serializer`, or null. */
    valueSerializer: Serializer<V> | null;
  });
  constructor(options?: unknown) {
    const form = CONSTRUCTOR_FORMS.match(options, arguments.length);
    let autoComplete = false;
    let keySerializer: Serializer<unknown> | null = null;
    let valueSerializer: Serializer<unknown> | null = null;
    if (form === 1) {
      const given = CONSTRUCTOR_FORMS.value(options, 'autoComplete');
      if (typeof given !== 'boolean') throw new TypeError('autoComplete must be a boolean');
      autoComplete = given;
      keySerializer = checkSerializer(CONSTRUCTOR_FORMS.value(options, 'keySerializer'), 'keySerializer');
      valueSerializer = checkSerializer(CONSTRUCTOR_FORMS.value(options, 'valueSerializer'), 'valueSerializer');
    }
    this.#keySerializer = keySerializer;
    this.#valueSerializer = valueSerializer;
    this.#handle = native.mockProducerCreate(autoComplete);
  }

  /**
   * Adds the record to the list of sent records.
   *
   * @param options - the record, and the callback to execute when it has
   *   been acknowledged
   * @returns resolving at hand-over to the future of the record's metadata,
   *   rejecting with the error the mock's send() throws
   * @throws `IllegalStateError` rejects with it when the mock is closed
   *   ("MockProducer is already closed.")
   * @throws `KafkaError` rejects with it when the mock is fenced
   * @throws `IllegalArgumentError` rejects with it when the given parameters
   *   are not `{record}` or `{record, callback}`, or a bare value is not a
   *   ProducerRecord
   * @throws `TypeError` rejects with it when `record` is null or not a
   *   ProducerRecord, `callback` not a function, or the topic holds a null
   *   byte
   *
   * @remarks The error set with `setSendException` rejects the send as it is,
   * the very same value. The serializers run first, key then value, and what
   * one throws rejects the send as it is *(deviation: Java's mock checks
   * that it is closed, fenced or has a send error before it serializes. Why:
   * the serializers run in JavaScript before the call reaches the core's
   * mock)*. With `autoComplete`, the callback runs inside the call, before
   * the returned Promise resolves; the record's headers stay writable, as
   * Java's mock leaves them.
   */
  send(options: {
    /** The record to send. */
    record: ProducerRecord<K, V>;
    /**
     * A user-supplied callback to execute when the record has been
     * acknowledged by the server (null indicates no callback).
     */
    callback: Callback | null;
  }): Promise<KafkaFuture<RecordMetadata>>;
  /**
   * Adds the record to the list of sent records, Java's `send(record)`: the
   * same as `send({ record, callback: null })`.
   *
   * @param options - the record
   * @returns resolving at hand-over to the future of the record's metadata
   */
  send(options: {
    /** The record to send. */
    record: ProducerRecord<K, V>;
    callback?: never;
  }): Promise<KafkaFuture<RecordMetadata>>;
  /**
   * Adds the record to the list of sent records: the record on its own, the
   * same as `send({record})`.
   *
   * @param record - The record to send
   * @returns resolving at hand-over to the future of the record's metadata
   */
  send(record: ProducerRecord<K, V>): Promise<KafkaFuture<RecordMetadata>>;
  send(options: unknown): Promise<KafkaFuture<RecordMetadata>> {
    try {
      return this.#send(options, arguments.length);
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the checks threw: an Error of the client's or JavaScript's (typed unknown by catch)
      return Promise.reject(error);
    }
  }

  // send()'s body: a throw is the returned Promise's rejection.
  #send(options: unknown, count: number): Promise<KafkaFuture<RecordMetadata>> {
    SEND_FORMS.match(options, count);
    const record = SEND_FORMS.value(options, 'record');
    if (record === null) throw new TypeError('record must not be null');
    if (!(record instanceof ProducerRecord)) throw new TypeError('record must be a ProducerRecord');
    const userCallback = checkCallback(SEND_FORMS.value(options, 'callback'));
    if (this.#keySerializer === null && this.#valueSerializer === null) return this.#handOver(record, userCallback);
    // Java's mock serializes with empty headers, its cluster having no
    // partition, and keeps the record as it was given. A serializer that
    // returns a plain value costs no microtask; one that returns a thenable
    // is awaited.
    const headers = new Headers();
    const key: unknown = this.#keySerializer?.serialize({ topic: record.topic, headers, data: record.key });
    if (isThenable(key)) return this.#sendAwaiting(record, userCallback, headers, key, true);
    const value: unknown = this.#valueSerializer?.serialize({ topic: record.topic, headers, data: record.value });
    if (isThenable(value)) return this.#sendAwaiting(record, userCallback, headers, value, false);
    return this.#handOver(record, userCallback);
  }

  // The rest of send() once a serializer returned a thenable: it awaits it,
  // runs the value serializer when the thenable was the key's, then hands
  // the record over.
  async #sendAwaiting(
    record: ProducerRecord<unknown, unknown>,
    userCallback: Callback | null,
    headers: Headers,
    pending: PromiseLike<unknown>,
    isKey: boolean,
  ): Promise<KafkaFuture<RecordMetadata>> {
    await pending;
    if (isKey && this.#valueSerializer !== null) {
      const value: unknown = this.#valueSerializer.serialize({ topic: record.topic, headers, data: record.value });
      if (isThenable(value)) await value;
    }
    return this.#handOver(record, userCallback);
  }

  // The hand-over of the app's record to the core's mock, then the drain of
  // the callbacks it queued, so a callback of an auto-completed send runs
  // inside the call, as Java's mock runs it.
  #handOver(record: ProducerRecord<unknown, unknown>, userCallback: Callback | null): Promise<KafkaFuture<RecordMetadata>> {
    const future: KafkaFuture<RecordMetadata> = KafkaFuture.internalCreate(true);
    let resolve!: (future: KafkaFuture<RecordMetadata>) => void;
    let reject!: (error: unknown) => void;
    const accepted = new Promise<KafkaFuture<RecordMetadata>>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    let inCall = true;
    const onHandover = (error: unknown, nativeFuture: number | null): void => {
      if (nativeFuture !== null) future.internalSetNative(this.#handle, nativeFuture);
      if (inCall && (this.#fastPathSends++ >= FAST_PATH_YIELD_EVERY || this.#deferred !== null)) {
        this.#deferSettlement(resolve, reject, future, error);
      } else if (!inCall && this.#deferred !== null) {
        this.#deferSettlement(resolve, reject, future, error);
      } else if (error !== null) {
        reject(error);
      } else {
        resolve(future);
      }
    };
    // The record's delivery callback, run inside the native drain: the user
    // callback first, then the delivery recorded on the future, which
    // completes it (javascript/CLAUDE.md, rules 5 and 6).
    const onComplete = (metadata: RecordMetadata, error: Error | null): string | undefined | Promise<string | undefined> => {
      let failure: string | undefined;
      let returned: unknown;
      if (userCallback !== null) {
        try {
          returned = userCallback({ metadata, exception: error });
          if (returned !== undefined) return completeWhenSettled(returned, future, metadata, error);
        } catch (thrown) {
          failure = thrownMessage(thrown);
        }
      }
      return completeRecord(future, metadata, error, failure);
    };
    const { topic, partition, timestamp } = record;
    try {
      native.mockProducerSend(this.#handle, topic, partition ?? -1, timestamp ?? -1n, record, onHandover, onComplete);
    } finally {
      inCall = false;
    }
    native.producerPump(this.#handle);
    return accepted;
  }

  // Settles a send's Promise once the event loop has turned, as
  // KafkaProducer's deferral does.
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
   * Completes every send still waiting, successfully, as `completeNext()`
   * does for each.
   *
   * @returns resolving once the sends completed and their callbacks ran
   * @throws `IllegalStateError` rejects with it when the mock is closed
   *   ("MockProducer is already closed.")
   * @throws `IllegalArgumentError` rejects with it when it is given an
   *   argument
   * @throws `TypeError` rejects with it when it is given `undefined`
   *
   * @remarks The error set with `setFlushException` rejects it as it is.
   */
  flush(): Promise<void>;
  flush(options?: unknown): Promise<void> {
    try {
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
   * The partitions of the topic in the mock's cluster: none, its cluster
   * being empty.
   *
   * @param options - the topic
   * @returns resolving to the topic's partitions, a new array
   * @throws `IllegalArgumentError` rejects with it when the given parameters
   *   are not `{topic}`
   * @throws `TypeError` rejects with it when `topic` is `undefined` or not a
   *   string
   *
   * @remarks The error set with `setPartitionsForException` rejects it as it
   * is.
   */
  partitionsFor(options: {
    /** The topic to get partition metadata for. */
    topic: string;
  }): Promise<PartitionInfo[]>;
  /**
   * The partitions of the topic: the topic on its own, the same as
   * `partitionsFor({topic})`.
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
   * The mock's metrics: none, as `setMockMetrics` is left out.
   *
   * @returns the mock's metrics, by name
   * @throws `TypeError` when it is given an argument
   */
  metrics(): MetricNameMap<Metric> {
    METRICS_FORMS.match(undefined, arguments.length);
    const entries: Array<[MetricName, Metric]> = native.producerMetrics(this.#handle).map((m) => {
      const name = new MetricName({ name: m.name, group: m.group, description: m.description, tags: new Map(m.tags) });
      return [name, KafkaMetric.internalSnapshot(name, m.value, m.measurable)];
    });
    return new MetricNameMap(entries);
  }

  /**
   * Closes the mock: Java's `close()`, which is `close(Duration.ofMillis(0))`.
   *
   * @returns resolving once the mock is closed
   * @throws `TypeError` rejects with it when it is given `undefined`
   *
   * @remarks The error set with `setCloseException` rejects it as it is,
   * and the mock stays open. Once closed, the mock no longer holds Node.
   */
  close(): Promise<void>;
  /**
   * Closes the mock; the timeout is not waited for.
   *
   * @param options - the timeout
   * @returns resolving once the mock is closed
   * @throws `IllegalArgumentError` rejects with it if the given parameters
   *   are not `{}` or `{timeout}`, or a bare value is neither a number nor a
   *   `Temporal.Duration`
   * @throws `TypeError` rejects with it when `timeout` is null, `undefined`,
   *   or neither a number nor a `Temporal.Duration`
   * @throws `RangeError` rejects with it when `timeout` is `NaN`,
   *   `-Infinity`, holds years, months or weeks, or is above
   *   `Number.MAX_SAFE_INTEGER` milliseconds
   */
  close(options: CloseArgs): Promise<void>;
  /**
   * Closes the mock: the timeout on its own, the same as `close({timeout})`.
   *
   * @param timeout - The timeout, not waited for
   * @returns resolving once the mock is closed
   */
  close(timeout: Duration): Promise<void>;
  close(options?: unknown): Promise<void> {
    let timeoutMs: number | bigint | null;
    try {
      const form = CLOSE_FORMS.match(options, arguments.length);
      timeoutMs = form === 0 ? null : durationToMillis(CLOSE_FORMS.value(options, 'timeout'), 'timeout');
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the checks threw: an Error of the client's or JavaScript's (typed unknown by catch)
      return Promise.reject(error);
    }
    return new Promise<void>((resolve, reject) => {
      native.producerClose(this.#handle, timeoutMs, (error) => {
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the error the core reported, a client Error src/internal/errors.ts built (typed unknown at the native boundary)
        if (error !== null) reject(error);
        else resolve();
      });
    });
  }

  /**
   * Closes the mock as `close()` does, so that a mock declared with `await
   * using` is closed when its scope ends.
   *
   * @returns resolving once the mock is closed, rejecting as `close()`
   *   rejects
   *
   * @remarks *(js-only)* JavaScript's async disposal protocol, the form Java's
   * `AutoCloseable.close()` takes in a `try`-with-resources block.
   */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  /**
   * Checks whether this mock producer has been closed.
   *
   * @returns true if the producer has been closed, false otherwise
   * @throws `TypeError` when it is given an argument
   */
  closed(): boolean {
    CLOSED_FORMS.match(undefined, arguments.length);
    return native.mockProducerState(this.#handle, 'closed');
  }

  /**
   * Checks whether all sent records have been completed (no pending
   * completions).
   *
   * @returns true if there are no pending completions, false otherwise
   * @throws `TypeError` when it is given an argument
   */
  flushed(): boolean {
    FLUSHED_FORMS.match(undefined, arguments.length);
    return native.mockProducerState(this.#handle, 'flushed');
  }

  /**
   * Get the list of sent records since the last call to `clear()`.
   *
   * @returns the sent records, the app's own, a new array
   * @throws `TypeError` when it is given an argument
   */
  history(): Array<ProducerRecord<K, V>> {
    HISTORY_FORMS.match(undefined, arguments.length);
    return native.mockProducerHistory(this.#handle) as Array<ProducerRecord<K, V>>;
  }

  /**
   * Clear the stored history of sent records, consumer group offsets.
   *
   * @throws `IllegalArgumentError` when it is given an argument
   * @throws `TypeError` when it is given `undefined`
   *
   * @remarks The sends still waiting are dropped too, as in Java: their
   * futures never complete.
   */
  clear(): void;
  clear(options?: unknown): void {
    CLEAR_FORMS.match(options, arguments.length);
    native.mockProducerClear(this.#handle);
  }

  /**
   * Complete the earliest uncompleted call successfully.
   *
   * @returns true if there was an uncompleted call to complete
   * @throws `IllegalArgumentError` when it is given an argument
   * @throws `TypeError` when it is given `undefined`
   *
   * @remarks The record's callback runs inside the call, and its future is
   * done when it returns.
   */
  completeNext(): boolean;
  completeNext(options?: unknown): boolean {
    COMPLETE_NEXT_FORMS.match(options, arguments.length);
    const completed = native.mockProducerCompleteNext(this.#handle, null);
    native.producerPump(this.#handle);
    return completed;
  }

  /**
   * Complete the earliest uncompleted call with the given error.
   *
   * @param options - the error
   * @returns true if there was an uncompleted call to complete
   * @throws `IllegalArgumentError` when the given parameters are not `{e}`
   * @throws `TypeError` when `e` is neither an `Error` nor null, or
   *   `undefined`
   *
   * @remarks The record's callback gets the very same error, and its future's
   * `get()` rejects with an `ExecutionError` whose `cause` it is. A null `e`
   * completes the call successfully, as Java's `completeNext()` is
   * `errorNext(null)`.
   */
  errorNext(options: {
    /** The error to complete the call with. */
    e: Error | null;
  }): boolean;
  /**
   * Complete the earliest uncompleted call with the given error: the error on
   * its own, the same as `errorNext({e})`.
   *
   * @param e - The error to complete the call with
   * @returns true if there was an uncompleted call to complete
   */
  errorNext(e: Error | null): boolean;
  errorNext(options: unknown): boolean {
    ERROR_NEXT_FORMS.match(options, arguments.length);
    const e = checkError(ERROR_NEXT_FORMS.value(options, 'e'), 'e');
    const completed = native.mockProducerCompleteNext(this.#handle, e);
    native.producerPump(this.#handle);
    return completed;
  }

  /**
   * Error to throw when `send` is called: Java's public field
   * `sendException`.
   *
   * @param options - the error, or null for none
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{sendException}`
   * @throws `TypeError` when `sendException` is neither an `Error` nor null,
   *   or `undefined`
   *
   * @remarks Java's public mutable field is this setter *(deviation, as in
   * Python. Why: the client keeps its state in private fields, and the core's
   * mock has a setter only)*. `send` rejects with the very same value.
   */
  setSendException(options: {
    /** Error to throw when `send` is called, or null. */
    sendException: Error | null;
  }): void;
  /**
   * Error to throw when `send` is called: the error on its own, the same as
   * `setSendException({sendException})`.
   *
   * @param sendException - The error, or null for none
   */
  setSendException(sendException: Error | null): void;
  setSendException(options: unknown): void {
    SET_SEND_FORMS.match(options, arguments.length);
    native.mockProducerSetError(this.#handle, 'send', checkError(SET_SEND_FORMS.value(options, 'sendException'), 'sendException'));
  }

  /**
   * Error to throw when `flush` is called: Java's public field
   * `flushException`.
   *
   * @param options - the error, or null for none
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{flushException}`
   * @throws `TypeError` when `flushException` is neither an `Error` nor null,
   *   or `undefined`
   *
   * @remarks Java's public mutable field is this setter *(deviation, as in
   * Python)*. `flush` rejects with the very same value.
   */
  setFlushException(options: {
    /** Error to throw when `flush` is called, or null. */
    flushException: Error | null;
  }): void;
  /**
   * Error to throw when `flush` is called: the error on its own.
   *
   * @param flushException - The error, or null for none
   */
  setFlushException(flushException: Error | null): void;
  setFlushException(options: unknown): void {
    SET_FLUSH_FORMS.match(options, arguments.length);
    native.mockProducerSetError(this.#handle, 'flush', checkError(SET_FLUSH_FORMS.value(options, 'flushException'), 'flushException'));
  }

  /**
   * Error to throw when `partitionsFor` is called: Java's public field
   * `partitionsForException`.
   *
   * @param options - the error, or null for none
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{partitionsForException}`
   * @throws `TypeError` when `partitionsForException` is neither an `Error`
   *   nor null, or `undefined`
   *
   * @remarks Java's public mutable field is this setter *(deviation, as in
   * Python)*. `partitionsFor` rejects with the very same value.
   */
  setPartitionsForException(options: {
    /** Error to throw when `partitionsFor` is called, or null. */
    partitionsForException: Error | null;
  }): void;
  /**
   * Error to throw when `partitionsFor` is called: the error on its own.
   *
   * @param partitionsForException - The error, or null for none
   */
  setPartitionsForException(partitionsForException: Error | null): void;
  setPartitionsForException(options: unknown): void {
    SET_PARTITIONS_FOR_FORMS.match(options, arguments.length);
    native.mockProducerSetError(
      this.#handle,
      'partitionsFor',
      checkError(SET_PARTITIONS_FOR_FORMS.value(options, 'partitionsForException'), 'partitionsForException'),
    );
  }

  /**
   * Error to throw when `close` is called: Java's public field
   * `closeException`.
   *
   * @param options - the error, or null for none
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{closeException}`
   * @throws `TypeError` when `closeException` is neither an `Error` nor null,
   *   or `undefined`
   *
   * @remarks Java's public mutable field is this setter *(deviation, as in
   * Python)*. `close` rejects with the very same value, and the mock stays
   * open.
   */
  setCloseException(options: {
    /** Error to throw when `close` is called, or null. */
    closeException: Error | null;
  }): void;
  /**
   * Error to throw when `close` is called: the error on its own.
   *
   * @param closeException - The error, or null for none
   */
  setCloseException(closeException: Error | null): void;
  setCloseException(options: unknown): void {
    SET_CLOSE_FORMS.match(options, arguments.length);
    native.mockProducerSetError(this.#handle, 'close', checkError(SET_CLOSE_FORMS.value(options, 'closeException'), 'closeException'));
  }
}
