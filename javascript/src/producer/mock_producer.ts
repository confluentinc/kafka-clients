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

// Java's MockProducer over the C FFI's MockProducer (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Implementation over the FFI): the mock
// is the core's, reached through the addon as KafkaProducer is, and the
// records it keeps are the app's own: each key and value crosses as the FFI's
// `void *`, which the FFI's mock keeps without reading, and the addon holds
// the record until clear() or the mock is destroyed (native/producer.c,
// MockRecord). The serializers the mock is given run here, on every send, as
// Java's mock runs them.
//
// Left out (Scope): Java's MockProducer(Cluster, boolean, Partitioner,
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
 * @remarks *(deviation)* Over the core's mock: callbacks run one at a time,
 * `flush()` completes every record, and a throwing callback's future
 * completes. Close every mock.
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
   * @remarks The cluster is always empty, so the partitioner is never called.
   * A serializer runs on every send, with fresh empty headers.
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
    // eslint-disable-next-line @typescript-eslint/no-redundant-type-constituents -- javascript/CLAUDE.md, JavaScript Binding Conventions, Scope types it Partitioner | null: Partitioner is a placeholder of unknown until it is translated
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
   * @remarks *(deviation)* The serializers and the null check run before the
   * closed check.
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
   * @remarks The mock completes every send still waiting, successfully, as
   * `completeNext()` does for each, and rejects with `IllegalStateError`
   * "MockProducer is already closed." when it is closed. The error set with
   * `setFlushException` rejects it as it is. A callback that throws rejects
   * it, after every waiting record completed *(deviation: Java's stops at
   * the first throw. Why: the core's mock completes them all in one call.
   * Goes when: the core's mock stops at the first throw)*. It completes the
   * waiting records and settles once their callbacks ran, one at a time, and
   * every callback Promise already started has, those `completeNext()` and
   * `errorNext()` started included, also when an error is kept from earlier;
   * a throw, else the oldest error kept (a callback Promise's rejection, or
   * the throw of a callback that waited for its turn), is then the
   * rejection, the others kept for later calls. Made from one of the mock's
   * callbacks, it waits for that callback, so a callback that awaits it
   * never settles, unless an error is set with `setFlushException`, which it
   * rejects with at once; `KafkaProducer` refuses it before the first
   * `await`, from a callback the core runs in its drain.
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
   * @remarks The mock answers the partitions of the topic in its cluster:
   * none, its cluster being empty. Java's mock answers `[]` for a null topic
   * *(deviation. Why: the FFI checks its mandatory parameters, and the
   * binding repeats no precondition. Goes when: the FFI's mock accepts a
   * null topic)*. The error set with `setPartitionsForException` rejects it
   * as it is.
   */
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

  /** @remarks The mock has no metrics, as `setMockMetrics` is left out. */
  metrics(): MetricNameMap<Metric> {
    METRICS_FORMS.match(undefined, arguments.length);
    const entries: Array<[MetricName, Metric]> = native.producerMetrics(this.#handle).map((m) => {
      const name = new MetricName({ name: m.name, group: m.group, description: m.description, tags: new Map(m.tags) });
      return [name, KafkaMetric.internalSnapshot(name, m.value, m.measurable)];
    });
    return new MetricNameMap(entries);
  }

  /**
   * @remarks The mock closes at once, as Java's mock does, a timeout given
   * not waited for. The error set with `setCloseException` rejects it, and the
   * mock stays open. It settles once the waiting callbacks ran, then rejects
   * with the oldest error kept. From a callback, before its first `await`, it
   * waits for no callback; after it, for that callback, so a callback that
   * awaits it never settles.
   */
  close(): Promise<void>;
  close(options: CloseArgs): Promise<void>;
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
   * @remarks Waiting sends are dropped.
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
   * @remarks *(deviation)* A waiting callback runs later.
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
   * @remarks *(deviation)* A waiting callback runs later, as for
   * `completeNext()`.
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
   * Error to throw when `send` is called.
   *
   * @param options - the error, or null for none
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{sendException}`
   * @throws `TypeError` when `sendException` is neither an `Error` nor null,
   *   or `undefined`
   *
   * @remarks *(deviation)* A setter.
   */
  setSendException(options: {
    /** Error to throw when `send` is called, or null. */
    sendException: Error | null;
  }): void;
  /**
   * Error to throw when `send` is called.
   *
   * @param sendException - The error on its own, or null for none
   */
  setSendException(sendException: Error | null): void;
  setSendException(options: unknown): void {
    SET_SEND_FORMS.match(options, arguments.length);
    native.mockProducerSetError(this.#handle, 'send', checkError(SET_SEND_FORMS.value(options, 'sendException'), 'sendException'));
  }

  /**
   * Error to throw when `flush` is called.
   *
   * @param options - the error, or null for none
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{flushException}`
   * @throws `TypeError` when `flushException` is neither an `Error` nor null,
   *   or `undefined`
   *
   * @remarks *(deviation)* A setter.
   */
  setFlushException(options: {
    /** Error to throw when `flush` is called, or null. */
    flushException: Error | null;
  }): void;
  /**
   * Error to throw when `flush` is called.
   *
   * @param flushException - The error on its own, or null for none
   */
  setFlushException(flushException: Error | null): void;
  setFlushException(options: unknown): void {
    SET_FLUSH_FORMS.match(options, arguments.length);
    native.mockProducerSetError(this.#handle, 'flush', checkError(SET_FLUSH_FORMS.value(options, 'flushException'), 'flushException'));
  }

  /**
   * Error to throw when `partitionsFor` is called.
   *
   * @param options - the error, or null for none
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{partitionsForException}`
   * @throws `TypeError` when `partitionsForException` is neither an `Error`
   *   nor null, or `undefined`
   *
   * @remarks *(deviation)* A setter.
   */
  setPartitionsForException(options: {
    /** Error to throw when `partitionsFor` is called, or null. */
    partitionsForException: Error | null;
  }): void;
  /**
   * Error to throw when `partitionsFor` is called.
   *
   * @param partitionsForException - The error on its own, or null for none
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
   * Error to throw when `close` is called.
   *
   * @param options - the error, or null for none; `close` rejects with it, and
   *   the mock stays open
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{closeException}`
   * @throws `TypeError` when `closeException` is neither an `Error` nor null,
   *   or `undefined`
   *
   * @remarks *(deviation)* A setter.
   */
  setCloseException(options: {
    /** Error to throw when `close` is called, or null. */
    closeException: Error | null;
  }): void;
  /**
   * Error to throw when `close` is called.
   *
   * @param closeException - The error on its own, or null for none
   */
  setCloseException(closeException: Error | null): void;
  setCloseException(options: unknown): void {
    SET_CLOSE_FORMS.match(options, arguments.length);
    native.mockProducerSetError(this.#handle, 'close', checkError(SET_CLOSE_FORMS.value(options, 'closeException'), 'closeException'));
  }
}
