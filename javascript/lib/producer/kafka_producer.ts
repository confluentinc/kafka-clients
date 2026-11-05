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
// keys and values cross as bytes (javascript/CLAUDE.md).

import native = require('../_native');
import { configEntries } from '../_config';
import { illegalArgument, illegalState } from '../_errors';
import { KafkaFuture } from '../common/kafka_future';
import type { Callback } from './callback';
import { ProducerRecord } from './producer_record';
import type { RecordMetadata } from './record_metadata';

/**
 * A producer's configuration: Java's ProducerConfig keys, as a plain object
 * or a `Map`. Values are stringified (`true` -> `"true"`, numbers and bigints
 * by `String`, arrays joined with `,`); `null` / `undefined` drop the key.
 *
 * @jsOnly The type of the configuration the constructor takes, Java's
 *   `Map<String, Object>` or `Properties`.
 */
export type ProducerConfigs =
  | Record<string, string | number | boolean | bigint | ReadonlyArray<string | number> | null | undefined>
  | Map<string, unknown>;

/**
 * Options of {@link KafkaProducer.close}.
 *
 * @jsOnly The options object of one Java overload set, `close()` and
 *   `close(Duration)` (javascript/CLAUDE.md "Java shape, JS idiom").
 */
export type CloseOptions = {
  /**
   * The maximum time to wait for the producer to complete any pending
   * requests, in milliseconds. A timeout of zero means do not wait.
   */
  timeoutMs?: number | undefined;
};

/** The native producer handle (`_native.ts`). */
type ProducerHandle = ReturnType<typeof native.producerCreate>;

// Java's KafkaProducer.throwIfProducerClosed message.
const CLOSED_MESSAGE = 'Cannot perform operation after producer has been closed';
// Java's KafkaProducer.close(Duration) message for a negative timeout.
const NEGATIVE_TIMEOUT_MESSAGE = 'The timeout cannot be negative.';

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
// Every this many such sends (the count goes on during the deferral), send()
// runs the callback drain itself on entry, so delivery callbacks run and their
// state is released in batches of at most this many records (Python's
// AsyncProducer._PUMP_EVERY).
const PUMP_EVERY = 64;

/** The callback `send` was given, or null; throws a TypeError for a non-function. */
function checkCallback(callback: unknown): Callback | null {
  if (callback === undefined || callback === null) return null;
  if (typeof callback !== 'function') throw new TypeError('callback must be a function (metadata, error) => void');
  return callback as Callback;
}

/** The text the core logs for a callback that threw (Java logs a throwing onCompletion). */
function thrownMessage(thrown: unknown): string {
  if (thrown instanceof Error) return `${thrown.name}: ${thrown.message}`;
  try {
    return String(thrown);
  } catch {
    return 'the callback threw a value that cannot be converted to a string';
  }
}

/**
 * A Kafka client that publishes records to the Kafka cluster.
 *
 * The producer is thread safe in Java; here every call is made from the JS
 * thread, and the delivery callbacks run there too: from the event loop, or
 * synchronously inside a later `send()` call, which runs those already due
 * before handing its record over (so a callback may run while the code that
 * called `send()` is mid-call).
 * Records are batched and sent by the client in the background; `send`
 * resolves once the record has reached the producer, with a future of the
 * record's metadata.
 */
export class KafkaProducer {
  #handle: ProducerHandle;
  // close() was called: send refuses records from then on.
  #closed = false;
  // The native producer was destroyed by a completed close.
  #destroyed = false;
  #closing: Promise<void> | null = null;
  // Sends whose hand-over completed inside the call since the event loop last
  // turned (as far as this producer saw: the deferral's setImmediate).
  #fastPathSends = 0;
  // Settlements waiting for the event-loop turn, or null when none is scheduled:
  // each a settle function, then the value it settles with.
  #deferred: unknown[] | null = null;

  /**
   * A producer is instantiated by providing a set of key-value pairs as
   * configuration. Valid configuration strings are documented in Java's
   * ProducerConfig.
   *
   * @param configs - The producer configs
   * @throws `KafkaError` when the configuration is invalid (`ConfigError`,
   *   also for a value of the wrong type for its key) or the producer cannot
   *   be built
   */
  constructor(configs: ProducerConfigs) {
    this.#handle = native.producerCreate(configEntries(configs));
  }

  /**
   * Asynchronously send a record to a topic and invoke the provided callback
   * when the send has been acknowledged.
   *
   * The send is asynchronous in two steps, as Java's `send()` returns a
   * `Future` once the record has been stored in the buffer of records waiting
   * to be sent:
   *
   * 1. The returned Promise resolves once the record has been handed to the
   *    producer (where Java's blocking `send()` returns: at once, or after
   *    waiting up to `max.block.ms` for metadata or buffer memory), with a
   *    {@link KafkaFuture} of the record's metadata. It rejects with the error
   *    Java's `send()` would throw (a closed producer, a bootstrap resolution
   *    failure, ...); such an error never reaches the callback.
   * 2. The future's `get()` resolves to the RecordMetadata assigned to the
   *    record once the record is acknowledged, or rejects with the error the
   *    delivery failed with. The callback, when given, runs first, on the JS
   *    thread. A future whose `get()` is never called raises no unhandled
   *    rejection.
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
   * A Buffer or Uint8Array key or value is borrowed, not copied, until the
   * returned Promise settles: it must not be modified or detached (transferred)
   * before then, and may be reused after. A string key or value is encoded
   * into a Buffer of the client's own and is unaffected.
   *
   * @param record - The record to send
   * @param callback - A user-supplied callback to execute when the record has
   *   been acknowledged by the server
   * @returns resolving at hand-over to the future of the record's metadata,
   *   rejecting with the error Java's send() would throw
   * @throws `IllegalStateError` when the producer is closed
   * @throws `TypeError` when `record` is not a ProducerRecord or `callback` not
   *   a function
   */
  send(record: ProducerRecord, callback?: Callback | null): Promise<KafkaFuture<RecordMetadata>> {
    if (!(record instanceof ProducerRecord)) throw new TypeError('record must be a ProducerRecord');
    const userCallback = checkCallback(callback);
    if (this.#closed) throw illegalState(CLOSED_MESSAGE);
    if (this.#fastPathSends % PUMP_EVERY === 0) {
      // A no-op inside a drain (a send from a delivery callback).
      native.producerPump(this.#handle);
      // A callback it ran may have closed the producer.
      if (this.#closed) throw illegalState(CLOSED_MESSAGE);
    }
    // `_toNative`, `_create` and `_complete` are private to the package's
    // users; bracket access is TypeScript's way for the package itself.
    const args = record['_toNative']();
    const future: KafkaFuture<RecordMetadata> = KafkaFuture['_create']();
    let resolve!: (future: KafkaFuture<RecordMetadata>) => void;
    let reject!: (error: unknown) => void;
    const handedOver = new Promise<KafkaFuture<RecordMetadata>>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    // The hand-over, inside producerSend for a record taken or refused at once,
    // later otherwise: an error is the one Java's send() would throw. Never
    // marked handled, so an un-awaited send that fails surfaces.
    let inCall = true;
    const settle = (error: unknown): void => {
      if (error !== null) reject(error);
      else resolve(future);
    };
    const onHandover = (error: unknown): void => {
      if (!inCall) {
        // Queued. Its completion may run in another send()'s pump, with no
        // event-loop turn since the count started: the count stays, and while
        // a deferral turn is pending it settles in that turn too, after the
        // sends already deferred (an await on it would otherwise resume in a
        // microtask, with no turn).
        if (this.#deferred !== null) this.#deferSettlement(resolve, reject, future, error);
        else settle(error);
      } else if (this.#fastPathSends++ >= FAST_PATH_YIELD_EVERY) {
        this.#deferSettlement(resolve, reject, future, error);
      } else {
        settle(error);
      }
    };
    // The record's delivery callback, run synchronously inside the native
    // drain: the user callback first (the only user code the drain runs),
    // then the future, whose get() Promises it settles (their reactions run
    // as microtasks after the drain). A message returned is what the callback
    // threw, for the core to log; the callback id the core reports by stays
    // in C (src/producer.c, send_on_completion).
    const onComplete = (metadata: RecordMetadata, error: Error | null): string | undefined => {
      let failure: string | undefined;
      if (userCallback !== null) {
        try {
          userCallback(metadata, error);
        } catch (thrown) {
          failure = thrownMessage(thrown);
        }
      }
      future['_complete'](metadata, error);
      return failure;
    };
    try {
      native.producerSend(this.#handle, ...args, onHandover, onComplete);
    } finally {
      inCall = false;
    }
    return handedOver;
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
   * Calling it from inside a callback rejects, as Java's flush() throws there.
   */
  flush(): Promise<void> {
    if (this.#destroyed) {
      // A completed close completed every record: Java's flush() then returns
      // at once, the accumulator being empty.
      return Promise.resolve();
    }
    return new Promise<void>((resolve, reject) => {
      native.producerFlush(this.#handle, (error) => (error !== null ? reject(error) : resolve()));
    });
  }

  /**
   * Close this producer. Waits until all previously sent requests complete
   * (`close()`), or up to `timeoutMs` for them (`close({timeoutMs})`, Java's
   * close(Duration)), then releases the native client. The records waited
   * for are those whose `send()` had resolved before `close()` was called.
   * Calling it again returns the close in progress, or resolves once the
   * producer is closed. A negative `timeoutMs` is rejected whatever the
   * producer's state and changes nothing, as Java's close(Duration) checks it
   * first.
   *
   * Await it before teardown: a producer whose close never completed is
   * force-closed, as by `close({timeoutMs: 0})`, when it is garbage collected
   * or its worker or process ends; its unsent records then fail, and no JS
   * callback or future runs for them.
   *
   * @param options - `timeoutMs`: the maximum time to wait for the producer
   *   to complete any pending requests. A timeout of zero means do not wait.
   * @returns rejecting with an `IllegalArgumentError` for a negative
   *   `timeoutMs`
   * @throws `TypeError` when `timeoutMs` is not an integer
   */
  close(options?: CloseOptions): Promise<void> {
    let timeoutMs: number | null = null;
    if (options !== undefined && options !== null) {
      if (typeof options !== 'object') throw new TypeError('close takes an options object {timeoutMs}');
      if (options.timeoutMs !== undefined && options.timeoutMs !== null) {
        if (typeof options.timeoutMs !== 'number' || !Number.isSafeInteger(options.timeoutMs)) {
          throw new TypeError('timeoutMs must be an integer');
        }
        timeoutMs = options.timeoutMs;
      }
    }
    // Java's close(Duration) rejects a negative timeout before anything else,
    // whatever the state. The core checks it too, but its verdict arrives at
    // the next drain, after this call marked the producer closed: checked here,
    // a rejected close changes nothing (javascript/CLAUDE.md, rule 1).
    if (timeoutMs !== null && timeoutMs < 0) return Promise.reject(illegalArgument(NEGATIVE_TIMEOUT_MESSAGE));
    if (this.#closing !== null || this.#destroyed) {
      return this.#closing !== null ? this.#closing : Promise.resolve();
    }
    this.#closed = true;
    this.#closing = new Promise<void>((resolve, reject) => {
      native.producerClose(this.#handle, timeoutMs, (error, destroyed) => {
        this.#closing = null;
        if (destroyed) {
          this.#destroyed = true;
        } else {
          // The core rejected the timeout and closed nothing (Java's
          // IllegalArgumentException): the producer stays open.
          this.#closed = false;
        }
        if (error !== null) reject(error);
        else resolve();
      });
    });
    return this.#closing;
  }
}
