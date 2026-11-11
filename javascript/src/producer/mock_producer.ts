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

// Java's MockProducer, a TypeScript translation of its Java source, not the
// FFI's mock (root CLAUDE.md, JavaScript Binding Conventions, Implementation
// over the FFI: temporary, as in Python, because the FFI's mock cannot hold
// the user's own records). Its methods of the producer interface take the
// forms and return the Promises KafkaProducer's do; its own methods are
// synchronous. Java's `synchronized` needs no lock: there is one thread.
//
// Java's mock completes a record in the call that completes it (send() with
// autoComplete, completeNext(), errorNext(), flush()): the callback runs
// there, then the record's future completes. What a callback throws
// propagates out of that call, and the record's future then never completes,
// as Java's Completion.complete calls onCompletion with no try/catch.
//
// Not translated: the constructor taking a Cluster, its only extra parameter
// a type the client does not translate (the forms Q30 leaves out are not
// decided yet; MockProducer(autoComplete, partitioner, keySerializer,
// valueSerializer) and MockProducer() pass Cluster.empty(), so the partition
// is always 0 and the partitioner is never called, as in Java);
// partitionsFor, partitionsForException, metrics and setMockMetrics, whose
// List and Map types and long metric values are not decided yet (Q25, Q23);
// the transaction members (initTransactions, beginTransaction,
// sendOffsetsToTransaction, commitTransaction, abortTransaction,
// fenceProducer, transactionInitialized, transactionInFlight,
// transactionCommitted, transactionAborted, sentOffsets, commitCount,
// uncommittedRecords, consumerGroupOffsetsHistory, uncommittedOffsets and
// their exception fields); clientInstanceId, disableTelemetry,
// injectTimeoutException, setClientInstanceId, registerMetricForSubscription,
// unregisterMetricFromSubscription and addedMetrics, as KafkaProducer has no
// clientInstanceId or metric subscription (no FFI entry point).

import { JavaForms } from '../internal/args';
import { isDurationOrNull } from '../internal/duration';
import { illegalState } from '../internal/errors';
import { checkSerializer, isThenable, serializedBytes } from '../internal/serde';
import { KafkaFuture } from '../common/kafka_future';
import { TopicPartition } from '../common/topic_partition';
import { Headers } from '../common/header/headers';
import type { Serializer } from '../common/serialization/serializer';
import type { Duration } from '../duration';
import type { Callback } from './callback';
import type { CloseArgs } from './kafka_producer';
import type { Partitioner } from './partitioner';
import { ProducerRecord } from './producer_record';
import { RecordMetadata } from './record_metadata';

// Java's forms of each method, in Java declaration order (src/internal/args.ts).
const CONSTRUCTOR_FORMS = new JavaForms('MockProducer', [['autoComplete', 'partitioner', 'keySerializer', 'valueSerializer'], []]);
const SEND_FORMS = new JavaForms('send', [['record'], ['record', 'callback']], {
  bareTypes: { record: (value) => value === null || value instanceof ProducerRecord },
});
const FLUSH_FORMS = new JavaForms('flush', [[]]);
const CLOSE_FORMS = new JavaForms('close', [[], ['timeout']], { bareTypes: { timeout: isDurationOrNull } });
const CLOSED_FORMS = new JavaForms('closed', [[]], { reader: true });
const FLUSHED_FORMS = new JavaForms('flushed', [[]], { reader: true });
const HISTORY_FORMS = new JavaForms('history', [[]], { reader: true });
const CLEAR_FORMS = new JavaForms('clear', [[]]);
const COMPLETE_NEXT_FORMS = new JavaForms('completeNext', [[]]);
const ERROR_NEXT_FORMS = new JavaForms('errorNext', [['e']]);

// Java's verifyNotClosed / send() message.
const CLOSED_MESSAGE = 'MockProducer is already closed.';
// RecordBatch.NO_TIMESTAMP; Integer.MAX_VALUE.
const NO_TIMESTAMP = -1;
const INT_MAX = 2 ** 31 - 1;

/** Java's MockProducer.Completion: a send the mock completes later. */
interface Completion {
  readonly offset: number;
  readonly metadata: RecordMetadata;
  readonly future: KafkaFuture<RecordMetadata>;
  readonly callback: Callback | null;
  readonly tp: TopicPartition;
}

/** The callback `send` was given, or null; throws a TypeError for a non-function. */
function checkCallback(callback: unknown): Callback | null {
  if (callback === undefined || callback === null) return null;
  if (typeof callback !== 'function') throw new TypeError('callback must be a function ({ metadata, exception }) => void');
  return callback as Callback;
}

/** The metadata Java's mock gives: offset `offset`, no timestamp, sizes `size`. */
function metadataOf(tp: TopicPartition, offset: number, size: number): RecordMetadata {
  return RecordMetadata.internalCreate({
    topic: tp.topic as string,
    partition: tp.partition,
    hasOffset: offset !== -1,
    offset,
    hasTimestamp: false,
    timestamp: NO_TIMESTAMP,
    serializedKeySize: size,
    serializedValueSize: size,
  });
}

/**
 * Java's `Completion.complete(e)`: the callback gets the record's metadata,
 * or Java's `RecordMetadata(tp, -1, -1, NO_TIMESTAMP, -1, -1)` and `e`; then
 * the record's future completes, with the offset, or `e`. A callback that
 * returns a Promise is awaited first, as KafkaProducer awaits one: the
 * returned Promise resolves once the future completed. What the callback
 * throws propagates, and the future then never completes, as in Java; a
 * rejection of its Promise is handled as that throw: the returned Promise
 * rejects with it, and the future never completes.
 */
function complete(completion: Completion, e: unknown): Promise<void> | undefined {
  const failed = e !== null && e !== undefined;
  const settle = (): void => {
    completion.future.internalComplete(failed ? metadataOf(completion.tp, -1, -1) : completion.metadata, failed ? e : null);
  };
  if (completion.callback !== null) {
    const metadata = failed ? metadataOf(completion.tp, -1, -1) : completion.metadata;
    const returned: unknown = completion.callback({ metadata, exception: failed ? (e as Error) : null });
    if (returned !== undefined) {
      return Promise.resolve(returned).then(settle);
    }
  }
  settle();
  return undefined;
}

/**
 * A mock of the producer interface you can use for testing code that uses
 * Kafka.
 *
 * By default this mock will synchronously complete each send call
 * successfully. However it can be configured to allow the user to control the
 * completion of the call and supply an optional error for the producer to
 * throw.
 *
 * @remarks Its producer methods (`send`, `flush`, `close`) return Promises,
 * as `KafkaProducer`'s do, so the mock stands in for it; a send completes,
 * and its callback runs, in the call that completes it. A callback that
 * throws, or returns a Promise that rejects, fails that call with its error,
 * and the record's future never completes, as Java's mock leaves it. It
 * emits no preview warning.
 */
export class MockProducer<K = Uint8Array, V = Uint8Array> {
  /**
   * Error to throw when `send` is called, or null. The same instance is
   * thrown (the returned Promise rejects with it).
   */
  sendException: Error | null = null;

  /** Error to throw when `flush()` is called, or null. */
  flushException: Error | null = null;

  /** Error to throw when `close()` is called, or null. */
  closeException: Error | null = null;

  readonly #autoComplete: boolean;
  readonly #partitioner: Partitioner;
  readonly #keySerializer: Serializer<unknown> | null;
  readonly #valueSerializer: Serializer<unknown> | null;
  readonly #sent: ProducerRecord<K | null, V | null>[] = [];
  readonly #completions: Completion[] = [];
  // The next offset of each topic partition, by its text (Java's
  // Map<TopicPartition, Long>, whose key has value equality).
  readonly #offsets = new Map<string, number>();
  #closed = false;
  // What the callback Promises that completeNext(), errorNext() and flush()
  // started rejected with, oldest first: each later send(), flush() or close()
  // raises the oldest one (Python's AsyncMockProducer._mock_callback_errors).
  readonly #callbackErrors: unknown[] = [];
  // The callback Promises started and not settled yet, each made never to
  // reject, which flush() and close() wait for (Python's _await_callbacks).
  readonly #running = new Set<Promise<void>>();

  /**
   * Create a mock producer.
   *
   * @param options - whether to complete every send at once, the partition
   *   strategy and the key and value serializers
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{autoComplete, partitioner, keySerializer, valueSerializer}` or none,
   *   or a serializer is a function that makes one ("did you forget '()'?")
   * @throws `TypeError` when `autoComplete` is not a boolean, a serializer is
   *   neither null nor an object with a `serialize` method, or a parameter is
   *   `undefined`
   *
   * @remarks Java's `MockProducer(autoComplete, partitioner, keySerializer,
   * valueSerializer)`, which passes `Cluster.empty()`: the partition is
   * always 0, and the partitioner is never called. A null serializer is not
   * called *(deviation, as Python's mock: Java's dereferences it)*.
   */
  constructor(options: {
    /**
     * If true automatically complete all requests successfully and execute
     * the callback. Otherwise the user must call `completeNext()` or
     * `errorNext(e)` after `send()` to complete the call and complete the
     * future that is returned.
     */
    autoComplete: boolean;
    /** The partition strategy, or null. */
    partitioner: Partitioner;
    /** The serializer for key that implements `Serializer`, or null. */
    keySerializer: Serializer<K> | null;
    /** The serializer for value that implements `Serializer`, or null. */
    valueSerializer: Serializer<V> | null;
  });
  /**
   * Create a new mock producer with invented metadata, Java's
   * `MockProducer()`: the same as `{ autoComplete: false, partitioner: null,
   * keySerializer: null, valueSerializer: null }`.
   */
  constructor();
  constructor(options?: unknown) {
    const form = CONSTRUCTOR_FORMS.match(options, arguments.length);
    if (form === 1) {
      this.#autoComplete = false;
      this.#partitioner = null;
      this.#keySerializer = null;
      this.#valueSerializer = null;
      return;
    }
    const autoComplete = CONSTRUCTOR_FORMS.value(options, 'autoComplete');
    if (typeof autoComplete !== 'boolean') throw new TypeError('autoComplete must be a boolean');
    this.#autoComplete = autoComplete;
    this.#partitioner = CONSTRUCTOR_FORMS.value(options, 'partitioner');
    this.#keySerializer = checkSerializer(CONSTRUCTOR_FORMS.value(options, 'keySerializer'), 'keySerializer');
    this.#valueSerializer = checkSerializer(CONSTRUCTOR_FORMS.value(options, 'valueSerializer'), 'valueSerializer');
  }

  /**
   * Adds the record to the list of sent records.
   *
   * @param options - the record, and the callback to execute when it has
   *   been completed
   * @returns resolving to the future of the record's metadata
   * @throws `IllegalStateError` rejects with it when the producer is closed
   * @throws `IllegalArgumentError` rejects with it when the given parameters
   *   are not `{record}` or `{record, callback}`
   * @throws `TypeError` rejects with it when `record` is null or not a
   *   ProducerRecord, `callback` is not a function, a parameter is
   *   `undefined`, or a serializer returns neither a Uint8Array nor null
   *
   * @remarks `sendException`, when set, is the rejection, the same instance.
   * The serializers run on the record's key and value with new, empty
   * headers, their bytes unused, as Java's mock runs them to surface a wrong
   * serializer; one returning a Promise is awaited. With `autoComplete`, the
   * record completes and its callback runs inside the call; what the
   * callback throws, or its Promise rejects with, is then the rejection, and
   * the record's future never completes. The record is typed
   * `ProducerRecord<K | null, V | null>`, as `KafkaProducer.send`'s is.
   */
  send(options: {
    /** The record to send. */
    record: ProducerRecord<K | null, V | null>;
    /** A user-supplied callback to execute when the record has been completed (null indicates no callback). */
    callback: Callback | null;
  }): Promise<KafkaFuture<RecordMetadata>>;
  /**
   * Adds the record to the list of sent records, Java's `send(record)`: the
   * same as `send({ record, callback: null })`.
   *
   * @param options - the record
   * @returns resolving to the future of the record's metadata
   */
  send(options: {
    /** The record to send. */
    record: ProducerRecord<K | null, V | null>;
    callback?: never;
  }): Promise<KafkaFuture<RecordMetadata>>;
  /**
   * Adds the record to the list of sent records: the record on its own, the
   * same as `send({record})`.
   *
   * @param record - The record to send
   * @returns resolving to the future of the record's metadata
   */
  send(record: ProducerRecord<K | null, V | null>): Promise<KafkaFuture<RecordMetadata>>;
  send(options: unknown): Promise<KafkaFuture<RecordMetadata>> {
    try {
      return this.#send(options, arguments.length);
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the checks, the injected error or the callback threw, as Java's mock throws it
      return Promise.reject(error);
    }
  }

  // send()'s body, in Java's order: the closed producer, the injected error,
  // then the record (Java dereferences it next).
  #send(options: unknown, count: number): Promise<KafkaFuture<RecordMetadata>> {
    SEND_FORMS.match(options, count);
    this.#throwDeferred();
    if (this.#closed) throw illegalState(CLOSED_MESSAGE);
    if (this.sendException !== null) throw this.sendException;
    const record = SEND_FORMS.value(options, 'record');
    if (record === null) throw new TypeError('record must not be null');
    if (!(record instanceof ProducerRecord)) throw new TypeError('record must be a ProducerRecord');
    const callback = checkCallback(SEND_FORMS.value(options, 'callback'));
    const typed = record as ProducerRecord<K | null, V | null>;
    // Java's cluster is empty: the serializers run only to throw what a wrong
    // one throws, with new headers.
    const key = this.#keySerializer === null ? null : this.#keySerializer.serialize({ topic: typed.topic, headers: new Headers(), data: typed.key });
    if (isThenable(key)) return this.#sendAwaiting(typed, callback, key, true);
    if (this.#keySerializer !== null) serializedBytes(key, 'keySerializer');
    return this.#sendAwaiting(typed, callback, null, false);
  }

  // The rest of send(): the value serializer, awaiting a thenable either
  // serializer returned, then the record is added and completed or queued.
  // Without a thenable it runs, and returns its settled Promise, inside the
  // call.
  #sendAwaiting(
    record: ProducerRecord<K | null, V | null>,
    callback: Callback | null,
    key: unknown,
    keyPending: boolean,
  ): Promise<KafkaFuture<RecordMetadata>> {
    if (keyPending) {
      return Promise.resolve(key).then((out) => {
        serializedBytes(out, 'keySerializer');
        return this.#sendAwaiting(record, callback, null, false);
      });
    }
    const value =
      this.#valueSerializer === null ? null : this.#valueSerializer.serialize({ topic: record.topic, headers: new Headers(), data: record.value });
    if (isThenable(value)) {
      return Promise.resolve(value).then((out) => {
        serializedBytes(out, 'valueSerializer');
        return this.#add(record, callback);
      });
    }
    if (this.#valueSerializer !== null) serializedBytes(value, 'valueSerializer');
    return this.#add(record, callback);
  }

  // Java's send() after the serializers: the record's partition 0 (the
  // cluster is empty), its offset, and its completion, at once with
  // autoComplete, else queued. It runs synchronously up to the await of a
  // callback's Promise: the callback runs inside send().
  async #add(record: ProducerRecord<K | null, V | null>, callback: Callback | null): Promise<KafkaFuture<RecordMetadata>> {
    // Closed while a serializer's Promise was pending.
    if (this.#closed) throw illegalState(CLOSED_MESSAGE);
    const tp = new TopicPartition({ topic: record.topic, partition: 0 });
    const future: KafkaFuture<RecordMetadata> = KafkaFuture.internalCreate(true);
    const offset = this.#nextOffset(tp);
    // Java's RecordMetadata(tp, baseOffset, batchIndex, NO_TIMESTAMP, 0, 0),
    // whose offset is baseOffset + batchIndex.
    const baseOffset = Math.max(0, offset - INT_MAX);
    const batchIndex = Math.min(INT_MAX, offset);
    const completion: Completion = { offset, metadata: metadataOf(tp, baseOffset + batchIndex, 0), future, callback, tp };
    this.#sent.push(record);
    if (this.#autoComplete) {
      const settled = complete(completion, null);
      // send() itself rejects with this callback's error; flush() and close()
      // still wait for the callback.
      if (settled !== undefined) await this.#track(settled, false);
    } else {
      this.#completions.push(completion);
    }
    return future;
  }

  // Java's nextOffset: 0 for a new topic partition, then one more each call.
  #nextOffset(tp: TopicPartition): number {
    const key = String(tp);
    const offset = this.#offsets.get(key);
    if (offset === undefined) {
      this.#offsets.set(key, 1);
      return 0;
    }
    this.#offsets.set(key, offset + 1);
    return offset;
  }

  /**
   * Completes every uncompleted call successfully, Java's mock's `flush()`.
   *
   * @returns resolving once every record is completed and the Promises its
   *   callback returned settled
   * @throws `IllegalStateError` rejects with it when the producer is closed
   * @throws `IllegalArgumentError` rejects with it when it is given an
   *   argument
   *
   * @remarks `flushException`, when set, is the rejection, the same
   * instance. It settles only once every callback Promise already started
   * (its own, and those `completeNext()` and `errorNext()` started) has
   * settled. What a callback throws is then the rejection, and the calls
   * queued behind it stay uncompleted; else the oldest error a callback
   * Promise rejected with, the others kept, in order, for later calls.
   */
  flush(): Promise<void>;
  flush(options?: unknown): Promise<void> {
    try {
      FLUSH_FORMS.match(options, arguments.length);
      this.#throwDeferred();
      if (this.#closed) throw illegalState(CLOSED_MESSAGE);
      if (this.flushException !== null) throw this.flushException;
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the checks or the injected error threw, as Java's mock throws it
      return Promise.reject(error);
    }
    let thrown: { error: unknown } | null = null;
    try {
      for (let completion = this.#completions.shift(); completion !== undefined; completion = this.#completions.shift()) {
        const settled = complete(completion, null);
        if (settled !== undefined) void this.#track(settled, true);
      }
    } catch (error) {
      // A callback that threw: the completions behind it stay queued, as
      // Java's flush() stops there.
      thrown = { error };
    }
    // Settles only once every callback already started has, its own and
    // those completeNext() and errorNext() started; then a callback that
    // threw, else the oldest kept error, is the rejection.
    return this.#awaitCallbacks().then(() => {
      if (thrown !== null) throw thrown.error;
      this.#throwDeferred();
    });
  }

  // Waits for every callback Promise started, those started meanwhile too.
  async #awaitCallbacks(): Promise<void> {
    while (this.#running.size > 0) await Promise.all([...this.#running]);
  }

  // Tracks a started callback's Promise until it settles: a rejection is
  // kept in order when `keep`, else the returned Promise rejects with it
  // (send()'s own callback). The tracked Promise never rejects.
  #track(settled: Promise<void>, keep: boolean): Promise<void> {
    let failure: { error: unknown } | null = null;
    const tracked: Promise<void> = settled.then(
      () => {
        this.#running.delete(tracked);
      },
      (error: unknown) => {
        this.#running.delete(tracked);
        if (keep) this.#callbackErrors.push(error);
        else failure = { error };
      },
    );
    this.#running.add(tracked);
    return tracked.then(() => {
      if (failure !== null) throw failure.error;
    });
  }


  /**
   * Closes the mock: later calls of `send()` and `flush()` reject with
   * `IllegalStateError` "MockProducer is already closed.".
   *
   * @throws `IllegalArgumentError` rejects with it when the given parameters
   *   are not `{}` or `{timeout}`
   * @throws `TypeError` rejects with it when it is given `undefined`
   *
   * @remarks `closeException`, when set, is the rejection, the same
   * instance, and the mock stays open. Java's mock closes no serializer. The
   * mock closes, then waits for every callback Promise already started; the
   * oldest error one rejected with is then the rejection.
   */
  close(): Promise<void>;
  /**
   * Closes the mock, Java's `close(Duration)`: the timeout is not read, as
   * Java's mock does not read it.
   *
   * @param options - the timeout
   */
  close(options: CloseArgs): Promise<void>;
  /**
   * Closes the mock: the timeout on its own, the same as `close({timeout})`.
   *
   * @param timeout - The maximum time to wait, which the mock does not read
   */
  close(timeout: Duration): Promise<void>;
  close(options?: unknown): Promise<void> {
    try {
      CLOSE_FORMS.match(options, arguments.length);
      if (this.closeException !== null) throw this.closeException;
      this.#closed = true;
      // Once the callbacks already started settled, the oldest kept error.
      return this.#awaitCallbacks().then(() => this.#throwDeferred());
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the check or the injected error threw, as Java's mock throws it
      return Promise.reject(error);
    }
  }

  /**
   * Closes the mock as `close()` does, so that a mock declared with
   * `await using` is closed when its scope ends.
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
   * @returns `true` if the producer has been closed, `false` otherwise
   * @throws `TypeError` when it is given an argument
   */
  closed(): boolean {
    CLOSED_FORMS.match(undefined, arguments.length);
    return this.#closed;
  }

  /**
   * Checks whether all sent records have been completed (no pending
   * completions).
   *
   * @returns `true` if there are no pending completions, `false` otherwise
   * @throws `TypeError` when it is given an argument
   */
  flushed(): boolean {
    FLUSHED_FORMS.match(undefined, arguments.length);
    return this.#completions.length === 0;
  }

  /**
   * Get the list of sent records since the last call to `clear()`
   *
   * @returns a new array of the records sent, the caller's own record
   *   objects, in send order
   * @throws `TypeError` when it is given an argument
   *
   * @remarks Provisional: an array for Java's `List`, the mapping the client
   * already uses, while how other collections are typed is not decided yet
   * (Q25); it may change when that is decided.
   */
  history(): ProducerRecord<K | null, V | null>[] {
    HISTORY_FORMS.match(undefined, arguments.length);
    return [...this.#sent];
  }

  /**
   * Clear the stored history of sent records, and the uncompleted calls.
   *
   * @throws `IllegalArgumentError` when it is given an argument
   */
  clear(): void {
    CLEAR_FORMS.match(undefined, arguments.length);
    this.#sent.length = 0;
    this.#completions.length = 0;
  }

  /**
   * Complete the earliest uncompleted call successfully.
   *
   * @returns true if there was an uncompleted call to complete
   * @throws `IllegalArgumentError` when it is given an argument
   *
   * @remarks The record's callback runs inside the call: what it throws
   * propagates, and the record's future then never completes. *(deviation,
   * as Python's `AsyncMockProducer`)* A callback's Promise that rejects does
   * so after this call returned: the record's future never completes, and
   * the error is kept, in order, each later `send()`, `flush()` or `close()`
   * raising the oldest one (`flush()` and `close()` first wait for the
   * callbacks already started). Why: the call is synchronous, as Java's, and
   * cannot wait for the Promise. Goes when: never.
   */
  completeNext(): boolean {
    COMPLETE_NEXT_FORMS.match(undefined, arguments.length);
    return this.#completeEarliest(null);
  }

  /**
   * Complete the earliest uncompleted call with the given error.
   *
   * @param options - the error
   * @returns true if there was an uncompleted call to complete
   * @throws `IllegalArgumentError` when the given parameters are not `{e}`
   * @throws `TypeError` when `e` is `undefined`
   *
   * @remarks A callback's Promise that rejects is kept as `completeNext()`
   * keeps it. The callback receives the error itself, and the future's `get()`
   * rejects with an `ExecutionError` whose `cause` it is. A null `e`
   * completes the call successfully, as Java's does.
   */
  errorNext(options: {
    /** The error to complete the call with. */
    e: Error | null;
  }): boolean;
  /**
   * Complete the earliest uncompleted call with the given error: the error
   * on its own, the same as `errorNext({e})`.
   *
   * @param e - The error to complete the call with
   * @returns true if there was an uncompleted call to complete
   */
  errorNext(e: Error | null): boolean;
  errorNext(options: unknown): boolean {
    ERROR_NEXT_FORMS.match(options, arguments.length);
    return this.#completeEarliest(ERROR_NEXT_FORMS.value(options, 'e'));
  }

  // Java's errorNext: the earliest completion, if any, completed with `e`.
  // A callback's Promise that rejects does so after this call returned: its
  // error is kept, in order, for the later send(), flush() or close() (Python's
  // AsyncMockProducer keeps it the same way).
  #completeEarliest(e: unknown): boolean {
    const completion = this.#completions.shift();
    if (completion === undefined) return false;
    const settled = complete(completion, e);
    if (settled !== undefined) void this.#track(settled, true);
    return true;
  }

  // Throws the oldest error a callback's Promise rejected with, if any.
  #throwDeferred(): void {
    if (this.#callbackErrors.length > 0) throw this.#callbackErrors.shift();
  }
}
