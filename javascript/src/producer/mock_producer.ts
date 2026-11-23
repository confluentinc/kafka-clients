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

/**
 * The native layer's node fields as a Node, Java's five-parameter constructor.
 *
 * @param fields - the node's fields, or null
 * @returns the node, or null
 */
function nodeOf(fields: { id: number; host: string | null; port: number; rack: string | null; isFenced: boolean } | null): Node | null {
  return fields === null ? null : new Node(fields);
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
 * An error to give the mock: an `Error`, or null; throws a TypeError for any other value.
 *
 * @param error - the error given
 * @param name - the argument's name, for the error
 * @returns the error, or null
 */
function checkError(error: unknown, name: string): Error | null {
  if (error === null || error instanceof Error) return error;
  throw new TypeError(`${name} must be an Error or null`);
}

/**
 * Records a record's delivery on its future after its callback ran, and gives back what the callback threw.
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
 * the delivery callbacks run on the JS thread, one at a time in completion
 * order, as `KafkaProducer`'s do, inside the mock call that completes the
 * record (`send()` with `autoComplete`, `completeNext()`, `errorNext()`,
 * `flush()`), before it returns, and what a callback throws comes out of
 * that call, as in Java. A callback that waits for its turn behind an
 * earlier callback's Promise runs once that settled, after `completeNext()`
 * or `errorNext()` returned: what it throws is then kept, as a rejection is
 * *(deviation, as Python's `AsyncMockProducer`: Java's runs it inside the
 * call. Why: a callback starts only once the one before it settled)*.
 * `history()` returns the app's own
 * records; the mock holds each until `clear()` or its native client is
 * released. Like a producer, a mock keeps Node running until `close()`
 * closes it, so close every mock, and a call made on a closed mock keeps Node
 * running until that call settles. The mock's cluster is always empty: Java's
 * constructor taking a `Cluster` is left out (Cluster is not translated), so
 * the partitioner is never called and every record goes to partition 0, as
 * in Java. A callback's throw leaves the record's future completed
 * *(deviation: Java's leaves it incomplete. Why: the core's mock completes
 * it. Goes when: the core's mock leaves it incomplete)*, and `flush()`
 * completes every waiting record, running each callback, before it rejects
 * with the first throw *(deviation: Java's `flush()` stops at the first
 * callback that throws, the later records staying waiting. Why: the core's
 * mock completes all of them in one call. Goes when: the core's mock stops at
 * the first throw)*. A callback's Promise that rejects is the callback's
 * error, as a throw is, and is never logged: `send()` with `autoComplete`
 * waits for it and rejects with it, and `flush()` rejects with it. After
 * `completeNext()` or `errorNext()` it is kept, oldest first, and the next
 * `send()`, `flush()` or `close()` rejects with the oldest, once
 * *(deviation, as Python's `AsyncMockProducer`: Java's mock throws it from
 * the completing call. Why: those calls return before the Promise
 * settles)*. Only the record's own `send()` gets its callback's error: a
 * `send()` made inside a callback, or one whose drain runs callbacks a
 * pending `flush()` completed, leaves those errors to their own calls.
 * With `autoComplete`, a `send()` made from a callback's synchronous part
 * runs its own callback in its turn, once that callback and the Promise it
 * returns settled, and settles after it, rejecting with its error, so a
 * callback that awaits it never settles *(deviation: Java's runs it inside
 * the nested `send()`. Why: the callback queue does not nest, and the
 * callbacks run one at a time. Goes when: a drain started inside a drain
 * runs the callbacks, where the addon's `cbq_drain` and the FFI's
 * `CallbackQueue::execute` return at once)*. A `completeNext()` or
 * `errorNext()` made there returns before the next record's callback runs,
 * which runs in its turn, its throw coming out of the outer completing call,
 * or kept when no such call runs it *(deviation: Java's nested call throws
 * it. Why: the callback queue does not nest, and those calls cannot wait.
 * Goes when: the same)*.
 * `flush()` and `close()` wait for the callbacks of the records they cover,
 * those waiting for their turn included, and for every callback Promise
 * already started, before they settle; the record's future completes once
 * the Promise settled, as for a throw. A `close()` made from one of the
 * mock's callbacks before its first `await` does not wait for the callbacks,
 * its own among them, as Python's `AsyncMockProducer`, but only from a
 * callback the core runs in its drain: one that waited for its turn behind
 * an earlier callback's Promise is as after its first `await`. After that
 * `await`, `close()` waits for that callback, and a `flush()` made from a
 * callback does too, unless an error is set with `setFlushException`, which
 * it rejects with at once, as Java's; so a callback that awaits either
 * otherwise never settles (`KafkaProducer` refuses a `flush()` made before
 * the first `await`, from a callback the core runs in its drain).
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
  // The delivery functions of the sends still waiting for their completion.
  // The addon holds each only weakly, so a closed mock nothing references is
  // collected and its native mock destroyed, while a completeNext() after
  // close() still runs the callback, as Java's does. clear() drops them, as
  // Java's drops the completions; the addon holds one strongly once its
  // record completed and its callback waits for its turn.
  readonly #pending = new Set<unknown>();
  // What a callback threw inside the completing call (send() with
  // autoComplete, completeNext(), errorNext(), flush()), which that call
  // throws, as Java's mock lets it propagate; the first one of the call.
  #callbackThrown: { readonly error: unknown } | null = null;
  // How many completing calls can still throw what a callback throws:
  // completeNext() and errorNext() while their drain runs, flush() until its
  // completion. A callback that waited for its turn behind another
  // callback's Promise runs once that settled (native/producer.c,
  // send_run_queued), after the completeNext() or errorNext() that completed
  // its record returned: with none, its throw is kept, as a rejection is.
  #completing = 0;
  // The rejections of the callback Promises, and the throws of the callbacks
  // that ran after their completing call, no call raised yet, oldest first:
  // each later send(), flush() or close() raises the oldest, once (Python's
  // AsyncMockProducer keeps them the same way).
  readonly #callbackErrors: unknown[] = [];
  // Every callback Promise started and not settled yet, each tracked by a
  // Promise that never rejects: flush() and close() wait for them all.
  readonly #running = new Set<Promise<void>>();
  // Whether the core's mock completes each record inside its send(), so a
  // record's callback run during its own send()'s hand-over is that send()'s.
  readonly #autoComplete: boolean;
  // How many user callbacks of this mock are running their synchronous part:
  // a close() made there does not wait for the callbacks, its own among them.
  #callbackDepth = 0;

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
   * send, each with its own fresh empty headers, and what it returns is not
   * kept, as in Java; a null one is not run.
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
    this.#autoComplete = autoComplete;
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
   * one throws rejects the send as it is, an `ApiError` or a `TypeError`
   * included, as Java's mock serializes with no `try` *(deviation: Java's mock checks
   * that it is closed, fenced or has a send error before it serializes. Why:
   * the serializers run in JavaScript before the call reaches the core's
   * mock)*. A null `record` rejects with `TypeError` even on a closed mock
   * *(deviation: Java's mock checks that it is closed first. Why: the
   * binding keeps no closed state, and the record is checked in JavaScript
   * before the call reaches the core's mock, which holds it, as for
   * `KafkaProducer.send`)*. With `autoComplete`, the callback runs inside the call, before
   * the returned Promise resolves, and a Promise it returns is awaited, its
   * rejection rejecting the send; made from a callback's synchronous part,
   * or while an earlier callback's Promise is pending, the send runs its
   * callback in its turn, once the callbacks before it settled, and only then
   * settles, so a callback that awaits a send it made never settles. The
   * record's headers stay writable, as Java's mock leaves them. An error a
   * callback's Promise rejected with after `completeNext()` or `errorNext()`,
   * or a callback that waited for its turn threw, rejects the next send,
   * once, before it sends anything.
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
    // The oldest error a callback's Promise rejected with, which no call
    // raised yet.
    const kept = this.#takeCallbackError();
    if (kept !== null) throw kept.error;
    const record = SEND_FORMS.value(options, 'record');
    if (record === null) throw new TypeError('record must not be null');
    if (!(record instanceof ProducerRecord)) throw new TypeError('record must be a ProducerRecord');
    const userCallback = checkCallback(SEND_FORMS.value(options, 'callback'));
    if (this.#keySerializer === null && this.#valueSerializer === null) return this.#handOver(record, userCallback);
    // Java's mock serializes with fresh empty headers, a new RecordHeaders for
    // each serializer, its cluster having no partition, and keeps the record
    // as it was given. A serializer that returns a plain value costs no
    // microtask; one that returns a thenable is awaited.
    const key: unknown = this.#keySerializer?.serialize({ topic: record.topic, headers: new Headers(), data: record.key });
    if (isThenable(key)) return this.#sendAwaiting(record, userCallback, key, true);
    const value: unknown = this.#valueSerializer?.serialize({ topic: record.topic, headers: new Headers(), data: record.value });
    if (isThenable(value)) return this.#sendAwaiting(record, userCallback, value, false);
    return this.#handOver(record, userCallback);
  }

  // The rest of send() once a serializer returned a thenable: it awaits it,
  // runs the value serializer when the thenable was the key's, then hands
  // the record over.
  async #sendAwaiting(
    record: ProducerRecord<unknown, unknown>,
    userCallback: Callback | null,
    pending: PromiseLike<unknown>,
    isKey: boolean,
  ): Promise<KafkaFuture<RecordMetadata>> {
    await pending;
    if (isKey && this.#valueSerializer !== null) {
      const value: unknown = this.#valueSerializer.serialize({ topic: record.topic, headers: new Headers(), data: record.value });
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
    // Whether the record's delivery ran (or the send was refused, so none
    // will), and the signal of it: a send() made from a callback's
    // synchronous part has its delivery run by the outer drain, once that
    // callback returned, as the callback queue does not nest.
    let delivered = false;
    let signalDelivered!: () => void;
    const delivery = new Promise<void>((res) => {
      signalDelivered = res;
    });
    const onHandover = (error: unknown, nativeFuture: number | null): void => {
      // A refused send has no completion: its delivery function goes.
      if (error !== null) {
        this.#pending.delete(onComplete);
        delivered = true;
        signalDelivered();
      }
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
    // With autoComplete the core's mock completes the record inside this
    // send(), so its callback is this send()'s, whichever drain runs it: this
    // send()'s own, or, for a send() made from a callback's synchronous part,
    // the outer one, which this send() waits for (as Java's nested send()
    // runs its callback), or, for a callback that waits for its turn behind
    // another callback's Promise, the run once that settled. Otherwise the
    // completing call is completeNext(), errorNext() or flush().
    const own = this.#autoComplete;
    // When this record's callback ran as this send()'s: what it threw, and
    // the outcome of the Promise it returned, which this send() rejects with.
    let ownThrown: { readonly error: unknown } | null = null;
    const started: Array<Promise<{ readonly error: unknown } | null>> = [];
    // The record's delivery callback, run inside the native drain: the user
    // callback first, then the delivery recorded on the future, which
    // completes it (javascript/CLAUDE.md, rules 5 and 6).
    const onComplete = (metadata: RecordMetadata, error: Error | null): string | undefined | Promise<string | undefined> => {
      this.#pending.delete(onComplete);
      try {
        let returned: unknown;
        if (userCallback !== null) {
          try {
            returned = this.#runCallback(userCallback, metadata, error);
            if (returned !== undefined) return this.#awaitCallback(returned, future, metadata, error, own ? started : null);
          } catch (thrown) {
            // Java's mock calls onCompletion with no try: the completing call
            // throws it (#rethrowCallback), and nothing logs it. A callback
            // that ran after its completing call returned, having waited for
            // its turn, cannot reach it: its throw is kept for the next
            // send(), flush() or close(), as a rejection is.
            if (own) ownThrown ??= { error: thrown };
            else if (this.#completing > 0) this.#callbackThrown ??= { error: thrown };
            else this.#callbackErrors.push(thrown);
          }
        }
        return completeRecord(future, metadata, error, undefined);
      } finally {
        delivered = true;
        signalDelivered();
      }
    };
    this.#pending.add(onComplete);
    // Java's mock with its empty cluster sends to partition 0 whatever the
    // record says: the core's mock gets no partition, so it reports 0, and
    // Java's offsets. history() returns the app's own record.
    const { topic, timestamp } = record;
    try {
      native.mockProducerSend(this.#handle, topic, -1, timestamp ?? -1n, record, onHandover, onComplete);
    } catch (thrown) {
      this.#pending.delete(onComplete);
      throw thrown;
    } finally {
      inCall = false;
    }
    native.producerPump(this.#handle);
    // Settles this send() once its record's callback ran as its own.
    const settle = (): Promise<KafkaFuture<RecordMetadata>> => {
      const thrown = ownThrown;
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the user's callback threw, whatever it is (typed unknown by catch)
      if (thrown !== null) return Promise.reject(thrown.error);
      if (started.length === 0) return accepted;
      // An auto-completed send's callback returned a Promise: send() settles
      // once it did, and rejects with its rejection, as with a throw.
      return Promise.all(started).then((failures) => {
        for (const failure of failures) if (failure !== null) throw failure.error;
        return accepted;
      });
    };
    // Read after the drain above, which TypeScript does not see.
    if (own && !(delivered as boolean)) {
      // A send() from a callback's synchronous part: the drain above ran
      // nothing, as the queue does not nest. The outer drain runs the
      // delivery once that callback returned; this send() settles after it.
      // So does a send() whose callback waits for its turn behind another
      // callback's Promise: it runs once that settled.
      accepted.catch(() => undefined);
      return delivery.then(settle);
    }
    return settle();
  }

  // Runs the user's callback, counted in #callbackDepth while its
  // synchronous part runs, so a close() it makes there does not wait for the
  // callbacks, its own among them (Python's AsyncMockProducer skips the wait
  // the same way, its _in_async_callback).
  #runCallback(userCallback: Callback, metadata: RecordMetadata, error: Error | null): unknown {
    this.#callbackDepth++;
    try {
      return userCallback({ metadata, exception: error });
    } finally {
      this.#callbackDepth--;
    }
  }

  // A callback's Promise, awaited as a throw would be: its rejection is the
  // callback's error, never logged. The send() that auto-completed the record
  // (own, the outcomes it waits for) rejects with it; otherwise it is kept,
  // oldest first, for the next send(), flush() or close() (#callbackErrors).
  // flush() and close() wait for it (#running). The record's future completes
  // once it settled, whatever the outcome (the recorded deviation of a
  // throw). The returned Promise, for the addon to report by, never rejects.
  #awaitCallback(
    returned: unknown,
    future: KafkaFuture<RecordMetadata>,
    metadata: RecordMetadata,
    error: Error | null,
    own: Array<Promise<{ readonly error: unknown } | null>> | null,
  ): Promise<undefined> {
    const outcome: Promise<{ readonly error: unknown } | null> = Promise.resolve(returned).then(
      () => null,
      (reason: unknown) => ({ error: reason }),
    );
    if (own !== null) own.push(outcome);
    const tracked: Promise<void> = outcome.then((failure) => {
      this.#running.delete(tracked);
      if (failure !== null && own === null) this.#callbackErrors.push(failure.error);
    });
    this.#running.add(tracked);
    return tracked.then(() => {
      completeRecord(future, metadata, error, undefined);
      return undefined;
    });
  }

  // Waits for every callback Promise started, those started meanwhile too,
  // unless the call was made from a callback of this mock (#callbackDepth),
  // which would wait for itself (Python's AsyncMockProducer._await_callbacks).
  async #awaitCallbacks(fromCallback: boolean): Promise<void> {
    if (fromCallback) return;
    while (this.#running.size > 0) await Promise.all([...this.#running]);
  }

  // The oldest kept callback error, taken, or null.
  #takeCallbackError(): { readonly error: unknown } | null {
    return this.#callbackErrors.length > 0 ? { error: this.#callbackErrors.shift() } : null;
  }

  // What a callback threw during the completing call, taken, or null.
  #takeCallbackThrown(): { readonly error: unknown } | null {
    const thrown = this.#callbackThrown;
    this.#callbackThrown = null;
    return thrown;
  }

  // The start of a completing call (completeNext(), errorNext(), flush()):
  // returns whether it is made from a callback's synchronous part. Such a call
  // runs no callback, as the callback queue does not nest: the outer
  // completing call runs them, so the nested one neither clears nor takes what
  // a callback threw, which stays the outer call's, its first throw winning, as
  // Java's first onCompletion throw ends the outer call.
  #beginCompleting(): boolean {
    const fromCallback = this.#callbackDepth > 0;
    if (!fromCallback) this.#callbackThrown = null;
    return fromCallback;
  }

  // completeNext() / errorNext() once the callbacks ran: throws what one threw.
  #rethrowCallback(): void {
    const thrown = this.#takeCallbackThrown();
    if (thrown !== null) throw thrown.error;
  }

  // completeNext() and errorNext(): the core's mock completes the earliest
  // waiting send, with `e` or successfully for null, and its callback runs in
  // the drain that follows, unless it waits for its turn behind another
  // callback's Promise, as one made from a callback's synchronous part does
  // behind that callback. Returns whether a send was waiting.
  #completeNext(e: Error | null): boolean {
    const fromCallback = this.#beginCompleting();
    this.#completing++;
    let completed: boolean;
    try {
      completed = native.mockProducerCompleteNext(this.#handle, e);
      native.producerPump(this.#handle);
    } finally {
      this.#completing--;
    }
    if (!fromCallback) this.#rethrowCallback();
    return completed;
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
   * @remarks The error set with `setFlushException` rejects it as it is. A
   * callback that throws rejects it, after every waiting record completed
   * *(deviation: Java's stops at the first throw. Why: the core's mock
   * completes them all in one call. Goes when: the core's mock stops at the
   * first throw)*. It completes the waiting records and settles once their
   * callbacks ran, one at a time, and every callback Promise already started
   * has, those `completeNext()` and `errorNext()` started included, also when
   * an error is kept from earlier; a throw, else the oldest error kept (a
   * callback Promise's rejection, or the throw of a callback that waited for
   * its turn), is then the rejection, the others kept for later calls. Made
   * from one of the mock's callbacks, it waits for that callback, so a
   * callback that awaits it never settles, unless an error is set with
   * `setFlushException`, which it rejects with at once; `KafkaProducer`
   * refuses it before the first `await`, from a callback the core runs in
   * its drain.
   */
  flush(): Promise<void>;
  flush(options?: unknown): Promise<void> {
    try {
      FLUSH_FORMS.match(options, arguments.length);
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the check threw: an Error of the client's (typed unknown by catch)
      return Promise.reject(error);
    }
    const fromCallback = this.#beginCompleting();
    if (!fromCallback) this.#completing++;
    return new Promise<void>((resolve, reject) => {
      native.producerFlush(this.#handle, (error) => {
        // A callback the flush completed threw: Java's flush() throws it. A
        // flush() made from a callback leaves it to the outer completing call.
        if (!fromCallback) this.#completing--;
        const thrown = fromCallback ? null : this.#takeCallbackThrown();
        // Settles once every callback Promise already started has, its own
        // and those completeNext() and errorNext() started.
        void this.#awaitCallbacks(fromCallback).then(() => {
          const failure = thrown ?? (error !== null ? { error } : this.#takeCallbackError());
          // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what a callback threw or its Promise rejected with, or the error the core reported (typed unknown)
          if (failure !== null) reject(failure.error);
          else resolve();
        });
      });
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
   * @throws `TypeError` rejects with it when `topic` is null (the FFI's
   *   "topic cannot be null"), `undefined` or not a string
   *
   * @remarks Java's mock answers `[]` for a null topic *(deviation. Why:
   * the FFI checks its mandatory parameters, and the binding repeats no
   * precondition. Goes when: the FFI's mock accepts a null topic)*. The
   * error set with `setPartitionsForException` rejects it as it
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
   * and the mock stays open. Once closed, the mock holds Node only while a
   * call made on it waits for its answer. It
   * settles once the callbacks waiting for their turn ran and every callback
   * Promise already started has, then rejects with the oldest error kept
   * that no call raised yet. Made from one of the mock's callbacks before its
   * first `await`, it does not wait for the callbacks, its own among them, as
   * Python's `AsyncMockProducer`, but only from a callback the core runs in
   * its drain: one that waited for its turn behind an earlier callback's
   * Promise is as after that `await`, where it waits for the callback, so a
   * callback that awaits it never settles, as on `KafkaProducer`.
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
    const fromCallback = this.#callbackDepth > 0;
    return new Promise<void>((resolve, reject) => {
      native.producerClose(this.#handle, timeoutMs, (error) => {
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the error the core reported, a client Error src/internal/errors.ts built (typed unknown at the native boundary)
        if (error !== null) return reject(error);
        // Once every callback Promise already started settled, the oldest
        // error one rejected with that no call raised yet.
        void this.#awaitCallbacks(fromCallback).then(() => {
          const failure = this.#takeCallbackError();
          // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what a callback's Promise rejected with, whatever it is
          if (failure !== null) reject(failure.error);
          else resolve();
        });
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
   * futures never complete. A completed record's callback still waiting for
   * its turn runs in its turn, as Java's ran inside the call that completed
   * the record.
   */
  clear(): void;
  clear(options?: unknown): void {
    CLEAR_FORMS.match(options, arguments.length);
    native.mockProducerClear(this.#handle);
    // The core's mock dropped every waiting completion: no callback of
    // theirs can run. A completed record's callback waiting for its turn is
    // the addon's to hold until it runs (native/producer.c, send_enqueue).
    this.#pending.clear();
  }

  /**
   * Complete the earliest uncompleted call successfully.
   *
   * @returns true if there was an uncompleted call to complete
   * @throws `IllegalArgumentError` when it is given an argument
   * @throws `TypeError` when it is given `undefined`
   * @throws what the record's callback threw, as Java's mock lets it
   *   propagate, unless the call is made from a callback's synchronous part
   *   or the callback waits for its turn
   *
   * @remarks The record's callback runs inside the call, and its future is
   * done when it returns. A Promise the callback returns is not awaited: its
   * rejection is kept for the next `send()`, `flush()` or `close()`. While an
   * earlier callback's Promise is pending, the record's callback waits for
   * its turn: it runs once that settled, after this call returned, and its
   * throw is kept the same way *(deviation, as Python's `AsyncMockProducer`:
   * Java's runs it inside the call. Why: the callbacks run one at a time)*.
   * Made from a callback's synchronous part, it returns before the record's
   * callback runs, which runs in its turn, its throw coming out of the outer
   * completing call, or kept when no such call runs it *(deviation: Java's
   * runs it inside the call. Why: the callback queue does not nest)*.
   */
  completeNext(): boolean;
  completeNext(options?: unknown): boolean {
    COMPLETE_NEXT_FORMS.match(options, arguments.length);
    return this.#completeNext(null);
  }

  /**
   * Complete the earliest uncompleted call with the given error.
   *
   * @param options - the error
   * @returns true if there was an uncompleted call to complete
   * @throws `IllegalArgumentError` when the given parameters are not `{e}`
   * @throws `TypeError` when `e` is neither an `Error` nor null, or
   *   `undefined`
   * @throws what the record's callback threw, as Java's mock lets it
   *   propagate, unless the call is made from a callback's synchronous part
   *   or the callback waits for its turn
   *
   * @remarks The record's callback gets the very same error, and its future's
   * `get()` rejects with an `ExecutionError` whose `cause` it is. A null `e`
   * completes the call successfully, as Java's `completeNext()` is
   * `errorNext(null)`. Made from a callback's synchronous part, or while an
   * earlier callback's Promise is pending, it returns before the record's
   * callback runs, as `completeNext()` does *(deviation)*.
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
    return this.#completeNext(e);
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
