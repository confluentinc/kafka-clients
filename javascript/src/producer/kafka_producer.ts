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

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('../internal/native');
import { JavaForms } from '../internal/args';
import { configEntries } from '../internal/config';
import { debug } from '../internal/debug';
import { durationToMillis, isDurationOrNull } from '../internal/duration';
import { illegalState } from '../internal/errors';
import { producerClosed, producerOpened, warnPreview } from '../internal/lifecycle';
import { KafkaFuture } from '../common/kafka_future';
import type { Duration } from '../duration';
import type { Callback } from './callback';
import { ProducerRecord } from './producer_record';
import type { RecordMetadata } from './record_metadata';

/**
 * A producer's configuration: Java's ProducerConfig keys, as a plain object
 * (a `Map` is refused). Values are written as the strings the core parses
 * (`true` as `"true"`, numbers in decimal, arrays joined with `,`); an
 * `undefined` value leaves the key out, and so does `null` unless the key's
 * validator rejects `null`, which throws `ConfigError`. The value of a key
 * Java's `ProducerConfig` does not define is a string, a number or a boolean.
 *
 * @remarks *(js-only)* The type of the configuration the constructor takes,
 * Java's `Map<String, Object>` and `Properties` as one.
 */
export type ProducerConfigs = Record<
  string,
  string | number | boolean | bigint | ReadonlyArray<string | number> | null | undefined
>;

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

// Java's KafkaProducer.throwIfProducerClosed message.
const CLOSED_MESSAGE = 'Cannot perform operation after producer has been closed';

// Java's forms of each method, in Java declaration order (src/internal/args.ts). The
// constructor's Map and Properties forms are the one `configs`, which is also
// taken bare, the configuration map itself; its forms taking serializers are
// not translated (the client has no serializers yet).
// A bare value picks its form by its type: the configs an object (a `Map` is
// then refused as the configuration), the record a ProducerRecord, the timeout
// a number or a Temporal.Duration; `null` is Java's null.
const CONSTRUCTOR_FORMS = new JavaForms('KafkaProducer', [['configs']], {
  bareIsPlainObject: true,
  bareTypes: { configs: (value) => typeof value === 'object' },
});
const SEND_FORMS = new JavaForms('send', [['record'], ['record', 'callback']], {
  bareTypes: { record: (value) => value === null || value instanceof ProducerRecord },
});
const CLOSE_FORMS = new JavaForms('close', [[], ['timeout']], { bareTypes: { timeout: isDurationOrNull } });
const FLUSH_FORMS = new JavaForms('flush', [[]]);

/** What the addon takes for a close with its timeout left out: Java's close(). */
const NO_TIMEOUT = -1;

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

/** The callback `send` was given, or null; throws a TypeError for a non-function. */
function checkCallback(callback: unknown): Callback | null {
  if (callback === undefined || callback === null) return null;
  if (typeof callback !== 'function') throw new TypeError('callback must be a function ({ metadata, exception }) => void');
  return callback as Callback;
}

/**
 * Completes a record's future after its callback ran (Java completes the
 * future after onCompletion returned) and gives back `failure`, the text of
 * what the callback threw, for the core to log.
 */
function completeRecord(
  future: KafkaFuture<RecordMetadata>,
  metadata: RecordMetadata,
  error: Error | null,
  failure: string | undefined,
): string | undefined {
  future.internalComplete(metadata, error);
  return failure;
}

/**
 * `Promise.resolve(returned)`, the callback's result, with the record's
 * future completed once it settled: resolving to undefined, or to the text of
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
 * producer, with a future of the record's metadata. No garbage collection
 * closes a producer: close every one (`close()`, or `await using`).
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
   *   also for a value of the wrong type for its key, for `key.serializer`
   *   and `value.serializer` and for a `kafkaJS` block) or the producer
   *   cannot be built
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{configs}`, or the configs are not an object
   * @throws `TypeError` when `configs` is null, `undefined` or not a plain
   *   object (a `Map` included), or a config key or value holds a null byte,
   *   which the C ABI would read as the end of the string
   *
   * @remarks Java's `Map` and `Properties` constructors are the one
   * `configs`; the forms taking serializers are not translated yet. The first
   * client a process builds emits one `ExperimentalWarning` while the client
   * is in preview.
   */
  constructor(options: {
    /** The producer configs: a plain object of Java's ProducerConfig keys. */
    configs: ProducerConfigs;
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
    CONSTRUCTOR_FORMS.match(options, arguments.length);
    this.#handle = native.producerCreate(configEntries(CONSTRUCTOR_FORMS.value(options, 'configs')));
    producerOpened();
    warnPreview();
    debug('KafkaProducer created');
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
   * @throws `IllegalStateError` rejects with it when the producer is closed
   * @throws `IllegalArgumentError` rejects with it when the given parameters
   *   are not `{record}` or `{record, callback}`, or a bare value is not a
   *   ProducerRecord, or a field of the record was reassigned to a value its
   *   constructor refuses (the constructor's error)
   * @throws `TypeError` rejects with it when `record` is null or not a
   *   ProducerRecord, `callback` not a function, a parameter is `undefined`,
   *   the topic or a
   *   header key holds a null byte, which the C ABI would read as the end of
   *   the string, or a field of the record was reassigned to a value its
   *   constructor refuses (the constructor's error)
   * @throws `RangeError` rejects with it when a field of the record was
   *   reassigned to a partition above 2^31 - 1 or a timestamp above 2^53 - 1
   * @throws `InterruptError` rejects with it if the thread is interrupted while
   *   blocked
   * @throws `SerializationError` rejects with it if the key or value are not
   *   valid objects given the configured serializers
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
   * A Buffer or Uint8Array key or value is copied during the call, so it may
   * be modified, reused or detached (transferred) as soon as `send()`
   * returns. A string key or value is encoded to UTF-8 as Java's
   * `StringSerializer` encodes it (a lone surrogate as `?`). The record's
   * headers become read-only once its fields are checked: a later `add` or
   * `remove` throws `IllegalStateError` "RecordHeaders has been closed.".
   * *(deviation: Java's `send()` seals them only after its metadata wait;
   * here they are sealed before it. Why: the headers cross the C ABI during
   * `send()`, before the core's metadata wait. Goes when: the core seals the
   * headers itself, after that wait.)*
   *
   * Every failure rejects the returned Promise, an argument check's and the
   * closed check's included: `send()` never throws.
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
    record: ProducerRecord;
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
    record: ProducerRecord;
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
  send(record: ProducerRecord): Promise<KafkaFuture<RecordMetadata>>;
  send(options: unknown): Promise<KafkaFuture<RecordMetadata>> {
    try {
      return this.#send(options, arguments.length);
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the checks threw: an Error of the client's or JavaScript's (typed unknown by catch)
      return Promise.reject(error);
    }
  }

  // send()'s body: a throw is the returned Promise's rejection. The checks
  // run in Java's order (Java's doSend checks the closed producer before it
  // reads the record): the form, the closed producer, then the arguments.
  #send(options: unknown, count: number): Promise<KafkaFuture<RecordMetadata>> {
    SEND_FORMS.match(options, count);
    if (this.#closed) throw illegalState(CLOSED_MESSAGE);
    const record = SEND_FORMS.value(options, 'record');
    // Java's doSend dereferences the record: a NullPointerException.
    if (record === null) throw new TypeError('record must not be null');
    if (!(record instanceof ProducerRecord)) throw new TypeError('record must be a ProducerRecord');
    const userCallback = checkCallback(SEND_FORMS.value(options, 'callback'));
    if (this.#pumpNext || this.#fastPathSends % PUMP_EVERY === 0) {
      this.#pumpNext = false;
      // A no-op inside a drain (a send from a delivery callback).
      debug('KafkaProducer pump');
      native.producerPump(this.#handle);
      // A callback it ran may have closed the producer.
      if (this.#closed) throw illegalState(CLOSED_MESSAGE);
    }
    const args = record.internalToNative();
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
    const onHandover = (error: unknown): void => {
      handedOver = true;
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
    // then the future, whose get() Promises it settles (their reactions run
    // as microtasks after the drain). A message returned is what the callback
    // threw, for the core to log; the callback id the core reports by stays
    // in C (native/producer.c, send_on_completion). A callback that returned
    // a thenable is awaited: the future completes, and the Promise returned
    // here settles, once the thenable did, and the core's report waits for
    // that (native/producer.c, send_defer). A callback returning nothing
    // allocates no Promise.
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
   * @throws `IllegalStateError` rejects with it once `close()` was called,
   *   with Java's closed-producer message
   * @throws `IllegalArgumentError` rejects with it when it is given an
   *   argument: Java's flush() takes none
   * @throws `TypeError` rejects with it when it is given `undefined`
   * @throws `InterruptError` rejects with it if the thread is interrupted while
   *   blocked
   * @throws `KafkaError` rejects with it if the method is invoked inside a
   *   `send` callback
   *
   * @remarks A callback that returned a Promise counts as run once it
   * settled, so a callback's Promise must not wait for this flush.
   * *(deviation)* Once `close()` was called, it rejects with
   * `IllegalStateError`, as the Python client's does. Why: Java's `flush()`
   * has no closed check, since its object outlives `close()`, and the native
   * producer here does not. Goes when: the native producer outlives
   * `close()`.
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
    // Python's KafkaProducer.flush: the native producer is released by a
    // close, so a flush from the moment close() is called is refused.
    if (this.#closed) return Promise.reject(illegalState(CLOSED_MESSAGE));
    return new Promise<void>((resolve, reject) => {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the error the core reported, a client Error src/internal/errors.ts built (typed unknown at the native boundary)
      native.producerFlush(this.#handle, (error) => (error !== null ? reject(error) : resolve()));
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
   * @remarks The close then releases the native client. The records waited
   * for are those whose `send()` had resolved before `close()` was called,
   * and their callbacks' Promises: a callback Promise that never settles
   * holds the close. Calling it again returns the close in progress, or
   * resolves once the producer is closed. Every failure rejects the returned
   * Promise: `close()` never throws.
   *
   * Close every producer: no garbage collection closes one. A producer never
   * closed keeps running until its worker or the process ends (a warning
   * says how many were never closed when the event loop empties), and is
   * then closed as by `close({timeout: 0})`: its unsent records fail, and no
   * JS callback or future runs for them. `await using` closes a producer at
   * the end of its scope (`[Symbol.asyncDispose]`). Do not call it from a
   * send callback: it waits for that callback.
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
   * @remarks On a producer already closing or closed the call returns that
   * close, before its timeout is read. A negative `timeout` on an open
   * producer is rejected and changes nothing. A callback Promise that never
   * settles holds the close past its timeout.
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
    // The closed client first, then the timeout (root CLAUDE.md, JavaScript
    // Binding Conventions, Signatures).
    if (this.#closing !== null || this.#destroyed) {
      return this.#closing !== null ? this.#closing : Promise.resolve();
    }
    let timeoutMs: number | bigint;
    try {
      timeoutMs = form === 0 ? NO_TIMEOUT : durationToMillis(CLOSE_FORMS.value(options, 'timeout'), 'timeout');
    } catch (error) {
      // The core checks the sign too, but its verdict arrives at the next
      // drain, after this call marked the producer closed: checked here, a
      // rejected close changes nothing (javascript/CLAUDE.md, rule 1).
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the checks threw: an Error of the client's or JavaScript's (typed unknown by catch)
      return Promise.reject(error);
    }
    this.#closed = true;
    producerClosed();
    debug('KafkaProducer closing, timeout %s', form === 0 ? 'none' : String(timeoutMs));
    this.#closing = new Promise<void>((resolve, reject) => {
      native.producerClose(this.#handle, timeoutMs, (error, destroyed) => {
        this.#closing = null;
        if (destroyed) {
          this.#destroyed = true;
          debug('KafkaProducer closed');
        } else {
          // The core rejected the timeout and closed nothing (Java's
          // IllegalArgumentException): the producer stays open.
          this.#closed = false;
          producerOpened();
        }
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the error the core reported, a client Error src/internal/errors.ts built (typed unknown at the native boundary)
        if (error !== null) reject(error);
        else resolve();
      });
    });
    return this.#closing;
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
