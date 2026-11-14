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

// Java's KafkaFuture, with two value sources behind one class, as the Python
// client's (python/confluent_kafka/common/kafka_future.py):
//
// - a record's future, the one `KafkaProducer.send` resolves to: Java's
//   FutureRecordMetadata, completed with the record's delivery once its
//   callback returned, as Java's; a wait started before that goes through the
//   FFI's `get_cb` or `get_with_timeout_cb` on the native future of the
//   record's hand-over (javascript/CLAUDE.md, rule 5), created on first use.
//   It is never cancelled (`cancel()` answers false), and a timed `get` that
//   times out rejects with the core's TimeoutError;
// - Java's KafkaFutureImpl: `completedFuture`, `allOf` and the dependants of
//   `thenApply` / `whenComplete`, which can be cancelled; cancelling one
//   cancels it and its own dependants, never its source.
//
// A future is plain state, completed on the JS thread: no Promise or closure
// is made, and no native wait started, until something waits on it (`get`,
// `toCompletionStage`) or depends on it (`thenApply`, `whenComplete`,
// `allOf`). What depends on it runs where it completes (inside the producer's
// callback drain for a record), or inside the registering call when it is
// already done, as Java's CompletableFuture runs them. Only a future with no
// native handle runs a timer, for a timed `get`.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('../internal/native');
import { JavaForms } from '../internal/args';
import { durationToMillis, isDurationOrNull } from '../internal/duration';
import { create } from '../internal/throwable';
import { CancellationError } from '../cancellation_error';
import type { Duration } from '../duration';
import { ExecutionError } from '../execution_error';
import { TimeoutError } from '../timeout_error';

const PENDING = 0;
const SUCCEEDED = 1;
const FAILED = 2;
// Cancelled on its own: its own cancel(), a CancellationError it was
// completed with directly, or allOf's result. Java's CompletableFuture then
// holds the CancellationException itself, and cancel() answers true.
const CANCELLED = 3;
// Cancelled as a dependant: its source was cancelled, or its own function or
// action threw a CancellationError. Java's CompletableFuture then holds a
// CompletionException around it: isCancelled() is true (KafkaFutureImpl's
// dependant check), cancel() false.
const CANCELLED_AS_DEPENDANT = 4;

// Node's timers hold at most 2^31 - 1 ms: a longer delay would fire at once
// (with a TimeoutOverflowWarning), so a longer get(timeout) of a future with
// no native handle waits in steps of at most this, rechecking its deadline.
const MAX_TIMER_MS = 2 ** 31 - 1;

/** The native producer handle a record's future waits through (native/producer.c), opaque to JavaScript. */
type ProducerHandle = object;

/** What runs when a future completes: its state and outcome (the value, or the failure). */
type Listener = (state: number, outcome: unknown) => void;

/**
 * A value `completedFuture` and `getNow` take on its own, as TypeScript types
 * it: anything but a plain object, which is always the options object
 * (`{ value }`, `{ valueIfAbsent }`). TypeScript cannot tell a plain object
 * from an instance of the caller's own class, so the type lists the values no
 * plain object can be: primitives, `null`, arrays, functions, and the
 * built-in classes `Map`, `Set`, `Date`, `ArrayBuffer`, `DataView` and the
 * typed arrays (`Buffer` included). An instance of any other class goes by
 * name in TypeScript; plain JavaScript also takes it on its own.
 */
type BareValue =
  | string
  | number
  | boolean
  | bigint
  | symbol
  | null
  | ReadonlyArray<unknown>
  | ((...args: never[]) => unknown)
  | ReadonlyMap<unknown, unknown>
  | ReadonlySet<unknown>
  | Date
  | ArrayBuffer
  | DataView
  | Int8Array
  | Uint8Array
  | Uint8ClampedArray
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | Float32Array
  | Float64Array
  | BigInt64Array
  | BigUint64Array;

/**
 * The check `completedFuture`'s bare overload makes of its value's type `U`,
 * as a rest parameter: none when `U` is a `BareValue`, else one `never`, which
 * no call gives, so a plain object, a generic `U` or `undefined` is refused.
 * `U` is inferred from the value alone, so it keeps its type (`number` for
 * `1`, a branded string, `'a' | (string & {})`).
 */
type BareCheck<U> = [U] extends [BareValue] ? [] : [never];

/** The values of `T` that may be passed bare (`BareValue`). */
type Bare<T> = Extract<T, BareValue>;

// The futures completed whose waiters have not run yet, each followed by its
// waiters, in completion order, up to `queued`. A completion a waiter causes
// (a dependant's) is queued and run by the loop already running, so a chain
// of dependants completes iteratively, at any length, as Java's
// CompletableFuture.postComplete runs a dependant's completion (NESTED). An
// explicit completion (`cancel`, `internalComplete`) runs its own waiters, and
// what they cause, before it returns, even inside another's waiter, as Java's
// does. The array keeps its storage between completions: a slot is reused,
// never reallocated, so completing a future with a waiter allocates nothing
// (a long chain takes 2 slots per link while it runs, given back afterwards).
const COMPLETIONS: unknown[] = [];
let queued = 0;
let draining = false;
// A drain that grew the queue past this many slots gives the storage back.
const KEPT_SLOTS = 1024;

/** `String(value)` that never throws (`Object.create(null)`, a throwing `toString`). */
function text(value: unknown): string {
  try {
    return String(value);
  } catch {
    try {
      return Object.prototype.toString.call(value);
    } catch {
      return 'a value that cannot be converted to a string';
    }
  }
}

/**
 * Whether a thrown value is a `CancellationError`: false when its class
 * cannot be read (a revoked Proxy, a `getPrototypeOf` trap that throws).
 */
function isCancellation(value: unknown): boolean {
  try {
    return value instanceof CancellationError;
  } catch {
    return false;
  }
}

/**
 * The message of an `ExecutionError` around `cause`, as its `(cause)` form
 * gives it: `''` for `null` (Java's null cause) and `undefined`, else its
 * `String()`, which here never throws.
 */
function causeText(cause: unknown): string {
  return cause === null || cause === undefined ? '' : text(cause);
}

/** Whether `state` is a cancellation, of either kind. */
function cancelled(state: number): boolean {
  return state >= CANCELLED;
}

// The Promises toCompletionStage() returned, one per future, made on first use
// (a field would cost every record's future a slot).
const STAGES = new WeakMap<object, Promise<unknown>>();

// Java's forms of each member, in Java declaration order (src/internal/args.ts).
// A plain object is always the options object, so a plain-object value goes by
// name (`completedFuture({ value: obj })`).
// A bare value picks its form by its type; `completedFuture` and `getNow` take
// any value that is not a plain object.
const isFunctionOrNull = (value: unknown): boolean => value === null || typeof value === 'function';
const COMPLETED_FUTURE_FORMS = new JavaForms('completedFuture', [['value']]);
const ALL_OF_FORMS = new JavaForms('allOf', [['futures']], { bareTypes: { futures: (value) => value === null || Array.isArray(value) } });
const TO_COMPLETION_STAGE_FORMS = new JavaForms('toCompletionStage', [[]]);
const THEN_APPLY_FORMS = new JavaForms('thenApply', [['function']], { bareTypes: { function: isFunctionOrNull } });
const WHEN_COMPLETE_FORMS = new JavaForms('whenComplete', [['action']], { bareTypes: { action: isFunctionOrNull } });
const CANCEL_FORMS = new JavaForms('cancel', [['mayInterruptIfRunning']], {
  bareTypes: { mayInterruptIfRunning: (value) => typeof value === 'boolean' },
});
const GET_FORMS = new JavaForms('get', [[], ['timeout']], { bareTypes: { timeout: isDurationOrNull } });
const GET_NOW_FORMS = new JavaForms('getNow', [['valueIfAbsent']]);
const IS_CANCELLED_FORMS = new JavaForms('isCancelled', [[]], { reader: true });
const COMPLETED_WITH_ERROR_FORMS = new JavaForms('isCompletedExceptionally', [[]], { reader: true });
const IS_DONE_FORMS = new JavaForms('isDone', [[]], { reader: true });

/** The JavaScript type of a value, as an argument error names it. */
function kind(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'an array';
  return typeof value;
}

/**
 * A function parameter: Java dereferences it when the source completes, so a
 * `null` is refused at the call (`TypeError`), as is a non-function.
 */
function checkFunction(value: unknown, name: string, shape: string): (...args: never[]) => unknown {
  if (value === null) throw new TypeError(`${name} must not be null`);
  if (typeof value !== 'function') throw new TypeError(`${name} must be a function ${shape}, not ${kind(value)}`);
  return value as (...args: never[]) => unknown;
}

/**
 * A flexible future which supports call chaining and other asynchronous
 * programming patterns.
 *
 * # Relation to `Promise`
 *
 * It is possible to obtain a `Promise` from a `KafkaFuture` by calling
 * `toCompletionStage()`. The returned `KafkaFuture` of `whenComplete` or
 * `thenApply` fails with an `ExecutionError` from `get()`, whereas the
 * `Promise` fails with the failure itself.
 *
 * @remarks The future `KafkaProducer.send` resolves to is Java's
 *   `FutureRecordMetadata`: it is never cancelled, so its `cancel()` returns
 *   false, and a timed `get()` that times out rejects with the client's
 *   message, "Timed out waiting for KafkaFuture after <ms> ms", a negative
 *   timeout printed as 0 ms *(deviation: Java's is "Timeout after waiting
 *   for <ms> ms.", with the value given. Why: the message is the Rust core's
 *   `KafkaFutureImpl::get_with_timeout`, which every future shares and the
 *   C FFI passes on. Goes when: the core gives Java's message)*. Every other future (`completedFuture`, `allOf`, the
 *   dependants of `thenApply` and `whenComplete`) is Java's
 *   `KafkaFutureImpl` and can be
 *   cancelled. Java's `get()` blocks; here it returns a Promise. A failed
 *   future rejects only the Promises its `get()` and `toCompletionStage()`
 *   returned: a failed future nobody waits on raises no unhandled rejection,
 *   and neither do its dependants, while a Promise `get()` returned and the
 *   caller dropped does, as any rejected Promise does. The function of
 *   `thenApply` and the action of `whenComplete` are called positionally,
 *   `function(value)` and `action(value, error)`, as Java calls
 *   `apply(a)` and `accept(a, b)`.
 *
 * @typeParam T - the result type
 */
export abstract class KafkaFuture<T> {
  /**
   * Makes `KafkaFuture` nominal: TypeScript checks it by name, not by shape.
   * Declared only, it holds no state.
   */
  private declare readonly kafkaFutureBrand: never;

  /**
   * A future is made by the client, `completedFuture` and `allOf`; a subclass
   * may call this constructor.
   *
   * @throws `TypeError` when called for `KafkaFuture` itself, which is
   *   abstract, as Java's is
   */
  constructor() {
    if (new.target === KafkaFuture) {
      throw new TypeError('KafkaFuture cannot be constructed directly; use KafkaFuture.completedFuture(...) or KafkaFuture.allOf(...)');
    }
  }

  /**
   * Returns a new KafkaFuture that is already completed with the given value.
   *
   * @param options - the value
   *
   * @remarks A value that is not a plain object may also be given on its own,
   * `completedFuture(value)`. A plain object is always the options object, so
   * it is passed by name, `completedFuture({ value: obj })`, as generic code
   * should always do. In TypeScript the bare value is a primitive, `null`, an
   * array, a function, a `Map`, `Set`, `Date`, `ArrayBuffer`, `DataView` or
   * typed array (`Buffer` included); an instance of another class goes by name
   * there, while plain JavaScript also takes it on its own.
   * @returns the completed future
   * @throws `IllegalArgumentError` when the given parameters are not `{value}`
   *   (a bare plain object included)
   * @throws `TypeError` when `value` is `undefined`
   */
  static completedFuture<U>(options: {
    /** The value. */
    value: U;
  }): KafkaFuture<U>;

  /**
   * Returns a new KafkaFuture that is already completed with the given value:
   * the value on its own, the same as `completedFuture({value})`: anything
   * but a plain object; in TypeScript a primitive, `null`, an array, a
   * function, a `Map`, `Set`, `Date`, `ArrayBuffer`, `DataView` or typed
   * array (any other object by name).
   *
   * @param value - The value
   * @param check - None: a value TypeScript cannot take bare (a plain object,
   *   a generic value) asks for one more argument, which no call can give
   * @returns the completed future
   */
  static completedFuture<U>(value: U, ...check: BareCheck<U>): KafkaFuture<U>;

  static completedFuture<U>(options: unknown): KafkaFuture<U> {
    COMPLETED_FUTURE_FORMS.match(options, arguments.length);
    const future = new KafkaFutureImpl<U>(false);
    future.internalComplete(COMPLETED_FUTURE_FORMS.value(options, 'value') as U, null);
    return future;
  }

  /**
   * Returns a new KafkaFuture that is completed when all the given futures
   * have completed. If any future throws an error, the returned future
   * returns it. If multiple futures throw an error, which one gets returned
   * is arbitrarily chosen.
   *
   * @param options - the futures
   * @returns the future completed with `null` once all have completed
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{futures}`
   * @throws `TypeError` when `futures` is not an array, or holds `null`,
   *   `undefined` or a value that is not a KafkaFuture
   */
  static allOf(options: {
    /** The futures, an array (Java's varargs). */
    futures: ReadonlyArray<KafkaFuture<unknown>>;
  }): KafkaFuture<void>;

  /**
   * Returns a new KafkaFuture that is completed when all the given futures
   * have completed: the array on its own, the same as `allOf({futures})`.
   *
   * @param futures - The futures
   * @returns the future completed with `null` once all have completed
   */
  static allOf(futures: ReadonlyArray<KafkaFuture<unknown>>): KafkaFuture<void>;

  static allOf(options: unknown): KafkaFuture<void> {
    ALL_OF_FORMS.match(options, arguments.length);
    const futures = ALL_OF_FORMS.value(options, 'futures');
    // Java's Arrays.stream(futures) dereferences the array, and each element.
    if (futures === null) throw new TypeError('futures must not be null');
    if (!Array.isArray(futures)) throw new TypeError(`futures must be an array of KafkaFuture, not ${kind(futures)}`);
    const sources: Array<KafkaFuture<unknown>> = [];
    for (let i = 0; i < futures.length; i++) {
      const future: unknown = futures[i];
      if (future === null || future === undefined) throw new TypeError(`futures must not contain ${String(future)}`);
      if (!(future instanceof KafkaFuture)) throw new TypeError(`futures must contain only KafkaFutures, not ${kind(future)}`);
      sources.push(future as KafkaFuture<unknown>);
    }
    return KafkaFutureImpl.internalAllOf(sources);
  }

  /**
   * Gets a `Promise` with the same completion properties as this
   * `KafkaFuture`. The returned Promise will settle when this future
   * completes and in the same way (with the same result or error, a
   * `CancellationError` once cancelled). Every call returns the same Promise.
   *
   * If you want to wait on the completion of a KafkaFuture you should use
   * `get()` or `getNow()`, rather than the Promise.
   *
   * @returns the Promise settling with this future, rejecting with the
   *   failure itself
   * @throws `IllegalArgumentError` rejects with it when it is given an
   *   argument: Java's toCompletionStage() takes none
   */
  abstract toCompletionStage(): Promise<T>;

  /**
   * Returns a new KafkaFuture that, when this future completes normally, is
   * executed with this futures's result as the argument to the supplied
   * function.
   *
   * The function may be invoked by the call to `thenApply` or it may be
   * invoked by the code that completes the future.
   *
   * @param options - the function
   * @returns the dependant future: completed with the function's result,
   *   failed with what the function throws, or with this future's failure
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{function}`
   * @throws `TypeError` when `function` is `null`, `undefined` or not a
   *   function
   */
  abstract thenApply<R>(options: {
    /** The function, called as `function(value)`. */
    function: KafkaFuture.BaseFunction<T, R>;
  }): KafkaFuture<R>;

  /**
   * Returns a new KafkaFuture that, when this future completes normally, is
   * executed with this futures's result as the argument to the supplied
   * function: the function on its own, the same as `thenApply({function})`.
   *
   * @param fn - The function, called as `fn(value)`
   * @returns the dependant future
   */
  abstract thenApply<R>(fn: KafkaFuture.BaseFunction<T, R>): KafkaFuture<R>;

  /**
   * Returns a new KafkaFuture with the same result or error as this future,
   * that executes the given action when this future completes.
   *
   * When this future is done, the given action is invoked with the result
   * (or null if none) and the error (or null if none) of this future as
   * arguments.
   *
   * The returned future is completed when the action returns. The supplied
   * action should not throw an error. However, if it does, the following
   * rules apply: if this future completed normally but the supplied action
   * throws an error, then the returned future completes with the supplied
   * action's error. Or, if this future completed with an error and the
   * supplied action throws an error, then the returned future completes with
   * this future's error.
   *
   * The action may be invoked by the call to `whenComplete` or it may be
   * invoked by the code that completes the future.
   *
   * @param options - the action
   * @returns the new future
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{action}`
   * @throws `TypeError` when `action` is `null`, `undefined` or not a
   *   function
   */
  abstract whenComplete(options: {
    /** The action to perform, called as `action(value, error)`. */
    action: KafkaFuture.BiConsumer<T | null, unknown>;
  }): KafkaFuture<T>;

  /**
   * Returns a new KafkaFuture with the same result or error as this future,
   * that executes the given action when this future completes: the action on
   * its own, the same as `whenComplete({action})`.
   *
   * @param action - The action to perform, called as `action(value, error)`
   * @returns the new future
   */
  abstract whenComplete(action: KafkaFuture.BiConsumer<T | null, unknown>): KafkaFuture<T>;

  /**
   * If not already completed, completes this future with a
   * `CancellationError`. Dependent futures that have not already completed
   * will also complete with it.
   *
   * @param options - the flag
   * @returns true if this call cancelled the future, or it was cancelled on
   *   its own before (Java's `CompletableFuture.cancel`); false once done
   *   otherwise, a dependant cancelled through its source included, and
   *   always for a record's future, which is never cancelled
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{mayInterruptIfRunning}`
   * @throws `TypeError` when `mayInterruptIfRunning` is not a boolean
   */
  abstract cancel(options: {
    /** Ignored, as Java's `CompletableFuture` ignores it. */
    mayInterruptIfRunning: boolean;
  }): boolean;

  /**
   * If not already completed, completes this future with a
   * `CancellationError`: the flag on its own, the same as
   * `cancel({mayInterruptIfRunning})`.
   *
   * @param mayInterruptIfRunning - Ignored, as Java's `CompletableFuture`
   *   ignores it
   * @returns true if this call cancelled the future, or it was cancelled on
   *   its own before
   */
  abstract cancel(mayInterruptIfRunning: boolean): boolean;

  /**
   * Waits if necessary for this future to complete, and then returns its
   * result.
   *
   * Every failure rejects the returned Promise: `get()` never throws.
   *
   * @returns resolving to the result value, rejecting with an
   *   `ExecutionError` whose `cause` is the failure
   * @throws `ExecutionError` rejects with it when this future completed with
   *   an error, its `cause`
   * @throws `CancellationError` rejects with it when this future was
   *   cancelled
   */
  abstract get(): Promise<T>;

  /**
   * Waits if necessary for at most the given time for this future to
   * complete, and then returns its result, if available (Java's
   * `get(long, TimeUnit)`).
   *
   * Every failure rejects the returned Promise, an argument check's
   * included: `get()` never throws.
   *
   * @param options - the timeout
   * @returns resolving to the result value, rejecting with an
   *   `ExecutionError` whose `cause` is the failure
   * @throws `ExecutionError` rejects with it when this future completed with
   *   an error, its `cause`
   * @throws `CancellationError` rejects with it when this future was
   *   cancelled
   * @throws `TimeoutError` rejects with it if the wait timed out: the root
   *   package's `TimeoutError`, Java's `java.util.concurrent.TimeoutException`
   * @throws `IllegalArgumentError` rejects with it when the given parameters
   *   are not `{}` or `{timeout}`
   * @throws `TypeError` rejects with it when `timeout` is `null`, `undefined`
   *   or neither a number nor a `Temporal.Duration`
   * @throws `RangeError` rejects with it when `timeout` is `NaN`, `-Infinity`,
   *   holds years, months or weeks, or is beyond `Number.MAX_SAFE_INTEGER`
   *   milliseconds
   */
  abstract get(options: {
    /**
     * The maximum time to wait, a number of milliseconds (`Infinity`: no
     * limit) or a `Temporal.Duration`, rounded down to whole milliseconds; a
     * negative or zero timeout does not wait.
     */
    timeout: Duration;
  }): Promise<T>;

  /**
   * Waits if necessary for at most the given time for this future to
   * complete: the timeout on its own, the same as `get({ timeout })`, Java's
   * `get(long, TimeUnit)`.
   *
   * @param timeout - The maximum time to wait
   * @returns resolving to the result value, rejecting with an
   *   `ExecutionError` whose `cause` is the failure
   */
  abstract get(timeout: Duration): Promise<T>;

  /**
   * Returns the result value (or throws any encountered error) if completed,
   * else returns the given valueIfAbsent.
   *
   * @param options - the value returned while this future is not done
   * @returns the result value, or `valueIfAbsent` while not done
   * @throws `ExecutionError` when this future completed with an error, its
   *   `cause`
   * @throws `CancellationError` when this future was cancelled
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{valueIfAbsent}` (a bare plain object included)
   * @throws `TypeError` when `valueIfAbsent` is `undefined`
   */
  abstract getNow(options: {
    /**
     * The value returned while this future is not done. A plain object is
     * always the options object, so it is passed by name.
     */
    valueIfAbsent: T | null;
  }): T | null;

  /**
   * Returns the result value (or throws any encountered error) if completed,
   * else returns the given valueIfAbsent: the value on its own, the same as
   * `getNow({valueIfAbsent})`: anything but a plain object; in TypeScript a
   * primitive, `null`, an array, a function, a `Map`, `Set`, `Date`,
   * `ArrayBuffer`, `DataView` or typed array (any other object by name).
   *
   * @param valueIfAbsent - The value returned while this future is not done
   * @returns the result value, or `valueIfAbsent` while not done
   */
  abstract getNow(valueIfAbsent: Bare<T | null>): T | null;

  /**
   * Returns true if this future was cancelled before it completed normally.
   *
   * @returns whether this future was cancelled; always false for a record's
   *   future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  abstract isCancelled(): boolean;

  /**
   * Returns true if this future completed with an error, in any way.
   *
   * @returns whether this future failed or was cancelled
   * @throws `TypeError` when it is given an argument: it only reads
   */
  abstract isCompletedExceptionally(): boolean;

  /**
   * Returns true if completed in any fashion: normally, exceptionally, or
   * via cancellation.
   *
   * @returns whether this future is done
   * @throws `TypeError` when it is given an argument: it only reads
   */
  abstract isDone(): boolean;

  /**
   * Completes the future: `error` null for success with `value`; a
   * `CancellationError` cancels it. Only the first completion counts. Called
   * by `KafkaProducer` (and the tests, as Java's `complete` /
   * `completeExceptionally`).
   *
   * @returns whether this call completed the future
   * @internal
   */
  abstract internalComplete(value: T, error: unknown): boolean;

  /**
   * Sets a record's future's native source: the producer's handle and the
   * slot of the record's native future, which a wait started before the
   * delivery reads. Called by
   * `KafkaProducer` at the hand-over, before the future is handed out.
   *
   * @internal
   */
  abstract internalSetNative(producer: ProducerHandle, slot: number): void;

  /**
   * Records a record's delivery, as its callback saw it, once the callback
   * ran, and completes the future with it, as Java completes the future once
   * onCompletion returned: from then on `get()`, `getNow()` and `isDone()`
   * answer from it, and the native future is read no more. Called by
   * `KafkaProducer`.
   *
   * @internal
   */
  abstract internalSetDelivery(value: T, error: unknown): void;

  /**
   * A pending future: a record's (Java's `FutureRecordMetadata`, never
   * cancelled) when `record` is true, else Java's `KafkaFutureImpl`. Called
   * by `KafkaProducer` and the tests.
   *
   * @internal
   */
  static internalCreate<T>(record: boolean): KafkaFuture<T> {
    return new KafkaFutureImpl<T>(record);
  }
}

/**
 * Java's `KafkaFutureImpl`, the class of every future the client makes, and
 * a record's future (Java's `FutureRecordMetadata`) over the native future
 * of its hand-over.
 */
class KafkaFutureImpl<T> extends KafkaFuture<T> {
  #state: number = PENDING;

  // The value, or the failure (a CancellationError once cancelled).
  #outcome: unknown = null;

  // While pending, what runs at completion, in pairs: a waiting Promise's
  // resolve and reject, or a dependant's listener and null.
  #waiters: Array<((value: never, outcome: never) => void) | null> | null = null;

  // Java's FutureRecordMetadata (a record's future) rather than KafkaFutureImpl.
  readonly #record: boolean;

  // A record's future: the native future of its hand-over, set at the
  // hand-over (null for every other future, and for a record's future the
  // tests build).
  #native: ProducerHandle | null = null;
  // Its slot number in that producer's table of record futures.
  #slot = -1;

  // The untimed native wait (`get_cb`) is in flight: one serves every waiter.
  #nativeWaiting = false;

  // A record's future (Java's FutureRecordMetadata) when `record` is true.
  constructor(record: boolean) {
    super();
    this.#record = record;
  }

  override toCompletionStage(args?: unknown): Promise<T> {
    if (arguments.length !== 0) {
      try {
        TO_COMPLETION_STAGE_FORMS.match(args, arguments.length);
      } catch (error) {
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the check threw: an Error of the client's (typed unknown by catch)
        return Promise.reject(error);
      }
    }
    let stage = STAGES.get(this) as Promise<T> | undefined;
    if (stage === undefined) {
      stage = new Promise<T>((resolve, reject) => {
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the future's failure, as Java's completion stage fails with it (an Error of the client's or of the caller's own function)
        this.#listen((state, outcome) => (state === SUCCEEDED ? resolve(outcome as T) : reject(outcome)));
      });
      STAGES.set(this, stage);
    }
    return stage;
  }

  override thenApply<R>(options: unknown): KafkaFuture<R> {
    THEN_APPLY_FORMS.match(options, arguments.length);
    const fn = checkFunction(THEN_APPLY_FORMS.value(options, 'function'), 'function', '(value) => result') as KafkaFuture.BaseFunction<
      T,
      R
    >;
    const dependant = new KafkaFutureImpl<R>(false);
    this.#listen((state, outcome) => {
      if (state !== SUCCEEDED) {
        dependant.#settleAsDependant(state, outcome);
        return;
      }
      let result: R;
      try {
        result = fn(outcome as T);
      } catch (error) {
        dependant.#completeWithError(error, true);
        return;
      }
      dependant.#settle(SUCCEEDED, result, true);
    });
    return dependant;
  }

  override whenComplete(options: unknown): KafkaFuture<T> {
    WHEN_COMPLETE_FORMS.match(options, arguments.length);
    const action = checkFunction(WHEN_COMPLETE_FORMS.value(options, 'action'), 'action', '(value, error) => void') as KafkaFuture.BiConsumer<
      T | null,
      unknown
    >;
    const dependant = new KafkaFutureImpl<T>(false);
    this.#listen((state, outcome) => {
      try {
        if (state === SUCCEEDED) action(outcome as T, null);
        else action(null, outcome);
      } catch (error) {
        if (state === SUCCEEDED) dependant.#completeWithError(error, true);
        else dependant.#settleAsDependant(state, outcome);
        return;
      }
      dependant.#settleAsDependant(state, outcome);
    });
    return dependant;
  }

  override cancel(options: unknown): boolean {
    CANCEL_FORMS.match(options, arguments.length);
    const mayInterruptIfRunning = CANCEL_FORMS.value(options, 'mayInterruptIfRunning');
    if (typeof mayInterruptIfRunning !== 'boolean') {
      throw new TypeError(`mayInterruptIfRunning must be a boolean, not ${kind(mayInterruptIfRunning)}`);
    }
    // Java's FutureRecordMetadata.cancel.
    if (this.#record) return false;
    if (this.#state === PENDING) this.#settle(CANCELLED, new CancellationError(), false);
    return this.#state === CANCELLED;
  }

  override get(options?: unknown): Promise<T> {
    if (arguments.length === 0) return this.#get();
    let millis: number | bigint;
    try {
      if (GET_FORMS.match(options, arguments.length) === 0) return this.#get();
      millis = durationToMillis(GET_FORMS.value(options, 'timeout'), 'timeout');
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with what the checks threw: an Error of the client's or JavaScript's (typed unknown by catch)
      return Promise.reject(error);
    }
    // Infinity: no limit.
    if (typeof millis === 'bigint') return this.#get();
    return this.#timedGet(millis);
  }

  override getNow(options: unknown): T | null {
    GET_NOW_FORMS.match(options, arguments.length);
    const valueIfAbsent = GET_NOW_FORMS.value(options, 'valueIfAbsent') as T | null;
    if (this.#state === PENDING) return valueIfAbsent;
    if (this.#state === SUCCEEDED) return this.#outcome as T;
    throw this.#failure();
  }

  override isCancelled(): boolean {
    if (arguments.length !== 0) IS_CANCELLED_FORMS.match(undefined, arguments.length);
    return cancelled(this.#state);
  }

  override isCompletedExceptionally(): boolean {
    if (arguments.length !== 0) COMPLETED_WITH_ERROR_FORMS.match(undefined, arguments.length);
    return this.#state >= FAILED;
  }

  override isDone(): boolean {
    if (arguments.length !== 0) IS_DONE_FORMS.match(undefined, arguments.length);
    return this.#state !== PENDING;
  }

  // Java's KafkaFutureImpl.toString():
  // `KafkaFuture{value=<value>,exception=<error>,done=<done>}`, `done` being
  // whether the value or the error is not null, as Java's.
  override toString(): string {
    const value = this.#state === SUCCEEDED ? this.#outcome : null;
    const error = this.#state >= FAILED ? this.#outcome : null;
    return `KafkaFuture{value=${text(value)},exception=${text(error)},done=${String(error !== null || value !== null)}}`;
  }

  /**
   * `allOf`'s future over `sources`, checked by `KafkaFuture.allOf`. A
   * source of another class (a caller's own subclass) is read through its
   * `whenComplete`.
   *
   * @internal
   */
  static internalAllOf(futures: ReadonlyArray<KafkaFuture<unknown>>): KafkaFuture<void> {
    const sources = futures.map((f) => (f instanceof KafkaFutureImpl ? f : KafkaFutureImpl.#adopt(f)));
    const result = new KafkaFutureImpl<void>(false);
    if (sources.length === 0) {
      result.#settle(SUCCEEDED, null, false);
      return result;
    }
    // Java's allOf relays on the sources' results, not on its own callbacks:
    // it completes once every source is done, whichever source's completion
    // runs it (a source may be done with its waiters still to run). `next` is
    // the first source not seen done; each run moves it past the done ones,
    // so all runs together read each source once. The failure is the first in
    // the sources' order (Java's is arbitrary); a cancellation cancels it.
    let next = 0;
    let failedState = PENDING;
    let failure: unknown = null;
    const onDone: Listener = () => {
      while (next < sources.length) {
        const source = sources[next];
        if (source.#state === PENDING) return;
        if (source.#state !== SUCCEEDED && failedState === PENDING) {
          failedState = source.#state;
          failure = source.#outcome;
        }
        next += 1;
      }
      if (failedState === PENDING) result.#settle(SUCCEEDED, null, true);
      // Java's allOf completes with the CancellationException itself.
      else result.#settle(cancelled(failedState) ? CANCELLED : FAILED, failure, true);
    };
    for (const source of sources) source.#listen(onDone);
    return result;
  }

  // A future of another class, as a KafkaFutureImpl completed by its
  // whenComplete: `action(value, error)`, the error null on success.
  static #adopt(future: KafkaFuture<unknown>): KafkaFutureImpl<unknown> {
    const adopted = new KafkaFutureImpl<unknown>(false);
    future.whenComplete((value, error) => {
      if (error === null) adopted.#settle(SUCCEEDED, value, false);
      else adopted.#completeWithError(error, false);
    });
    return adopted;
  }

  // An untimed wait: the settled answer, or a Promise settled at completion.
  #get(): Promise<T> {
    if (this.#state !== PENDING) return this.#answer();
    return new Promise<T>((resolve, reject) => {
      if (this.#waiters === null) this.#waiters = [];
      this.#waiters.push(resolve, reject);
      this.#waitNative();
    });
  }

  // A timed wait of `millis` ms (a negative or zero one does not wait).
  #timedGet(millis: number): Promise<T> {
    if (this.#state !== PENDING) return this.#answer();
    const handle = this.#native;
    if (handle !== null) return this.#nativeTimedGet(handle, this.#slot, millis);
    if (millis <= 0) return Promise.reject(this.#timedOut(millis));
    return new Promise<T>((resolve, reject) => {
      const deadline = performance.now() + millis;
      let timer: ReturnType<typeof setTimeout>;
      const onValue = (value: T): void => {
        clearTimeout(timer);
        resolve(value);
      };
      const onError = (error: unknown): void => {
        clearTimeout(timer);
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the error get() reports: an ExecutionError or a CancellationError
        reject(error);
      };
      const expire = (): void => {
        const left = deadline - performance.now();
        if (left > 0) {
          timer = setTimeout(expire, Math.min(Math.ceil(left), MAX_TIMER_MS));
          return;
        }
        const waiters = this.#waiters;
        if (waiters !== null) {
          const i = waiters.indexOf(onValue);
          if (i >= 0) waiters.splice(i, 2);
        }
        reject(this.#timedOut(millis));
      };
      if (this.#waiters === null) this.#waiters = [];
      this.#waiters.push(onValue, onError);
      timer = setTimeout(expire, Math.min(millis, MAX_TIMER_MS));
    });
  }

  // The answer of a done future's get().
  #answer(): Promise<T> {
    if (this.#state === SUCCEEDED) return Promise.resolve(this.#outcome as T);
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with an ExecutionError or a CancellationError, as Java's get() throws them
    return Promise.reject(this.#failure());
  }

  // What a done, failed future's get() and getNow() report: the failure as an
  // ExecutionError's cause, or the CancellationError itself, as Java's.
  #failure(): unknown {
    // Built as the core's errors are, so any thrown value is a cause; the
    // message is the (cause) form's, from a String() that never throws
    // (Java's ExecutionException(Throwable) message is cause.toString()).
    return cancelled(this.#state) ? this.#outcome : create(ExecutionError, causeText(this.#outcome), this.#outcome);
  }

  // Java's java.util.concurrent.TimeoutException of a timed-out get with no
  // native handle: no message from KafkaFutureImpl (CompletableFuture's),
  // FutureRecordMetadata's own for a record's future the tests build.
  #timedOut(millis: number): TimeoutError {
    return this.#record ? new TimeoutError(`Timeout after waiting for ${millis} ms.`) : new TimeoutError();
  }

  // A timed wait of a record's future, through the FFI's get_with_timeout_cb,
  // which keeps the time (a negative or zero timeout does not wait): the
  // core's TimeoutError when the wait timed out, FutureRecordMetadata's.
  #nativeTimedGet(handle: ProducerHandle, slot: number, millis: number): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      if (this.#waiters === null) this.#waiters = [];
      this.#waiters.push(resolve, reject);
      native.recordFutureGet(handle, slot, millis, (metadata, error, timedOut) => {
        if (!timedOut) {
          this.#completeNative(metadata, error);
          return;
        }
        const waiters = this.#waiters;
        if (waiters === null) return;
        const i = waiters.indexOf(resolve);
        if (i < 0) return;
        waiters.splice(i, 2);
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- rejects with the core's TimeoutError, as Java's get(timeout) throws it
        reject(error);
      });
    });
  }

  // Starts the untimed native wait of a record's future, once: its
  // completion completes the future (`#completeNative`).
  #waitNative(): void {
    const handle = this.#native;
    if (handle === null || this.#nativeWaiting) return;
    this.#nativeWaiting = true;
    native.recordFutureGet(handle, this.#slot, null, (metadata, error) => {
      this.#nativeWaiting = false;
      this.#completeNative(metadata, error);
    });
  }

  // A native wait completed: a record's future still pending (its delivery
  // reached no JavaScript) completes with the value the wait read.
  #completeNative(metadata: unknown, error: unknown): void {
    if (error === null) this.#settle(SUCCEEDED, metadata, false);
    else this.#settle(FAILED, error, false);
  }

  // Runs `listener` at completion, or now when already done.
  #listen(listener: Listener): void {
    if (this.#state !== PENDING) {
      listener(this.#state, this.#outcome);
      return;
    }
    if (this.#waiters === null) this.#waiters = [];
    this.#waiters.push(listener, null);
    this.#waitNative();
  }

  // Java's completeExceptionally: a CancellationError cancels, as Java's
  // CompletableFuture completed with a CancellationException is cancelled;
  // a dependant's own function or action throwing it cancels it as a
  // dependant.
  #completeWithError(error: unknown, dependant: boolean): boolean {
    if (!isCancellation(error)) return this.#settle(FAILED, error, dependant);
    return this.#settle(dependant ? CANCELLED_AS_DEPENDANT : CANCELLED, error, dependant);
  }

  // A dependant takes its source's outcome; a cancellation as a dependant.
  #settleAsDependant(state: number, outcome: unknown): void {
    this.#settle(cancelled(state) ? CANCELLED_AS_DEPENDANT : state, outcome, true);
  }

  // Completes a pending future, then runs what waits on it, in the order it
  // was registered. `nested`: a dependant's completion, made by a waiter of
  // its source; while a drain runs it is only queued, for that drain to run
  // (`COMPLETIONS`). Any other completion drains its own entry, and what it
  // causes, before it returns. Each waiter runs even when one before it threw,
  // the first throw being rethrown at the end.
  #settle(state: number, outcome: unknown, nested: boolean): boolean {
    if (this.#state !== PENDING) return false;
    this.#state = state;
    this.#outcome = outcome;
    const waiters = this.#waiters;
    this.#waiters = null;
    if (waiters === null) return true;
    const start = queued;
    COMPLETIONS[queued++] = this;
    COMPLETIONS[queued++] = waiters;
    if (!nested || !draining) KafkaFutureImpl.#drain(start);
    return true;
  }

  // Runs the queued completions from `start` until none is left, then frees
  // their slots; the entries before `start` belong to the drain this one runs
  // inside, which goes on with them once it returns.
  static #drain(start: number): void {
    const outer = draining;
    draining = true;
    let thrown = false;
    let first: unknown = null;
    try {
      for (let n = start; n < queued; n += 2) {
        const future = COMPLETIONS[n] as KafkaFutureImpl<unknown>;
        const waiters = COMPLETIONS[n + 1] as Array<((value: never, outcome: never) => void) | null>;
        COMPLETIONS[n] = null;
        COMPLETIONS[n + 1] = null;
        const state = future.#state;
        const outcome = future.#outcome;
        for (let i = 0; i < waiters.length; i += 2) {
          try {
            const reject = waiters[i + 1];
            if (reject === null) (waiters[i] as Listener)(state, outcome);
            else if (state === SUCCEEDED) (waiters[i] as (value: unknown) => void)(outcome);
            else (reject as (error: unknown) => void)(future.#failure());
          } catch (error) {
            if (!thrown) {
              thrown = true;
              first = error;
            }
          }
        }
        // Every entry from `start` has run: reuse their slots once all have
        // run. A chain queues each link before the one before it is done, so
        // it takes 2 slots per link, given back afterwards (KEPT_SLOTS).
        if (n + 2 === queued) {
          queued = start;
          n = start - 2;
        }
      }
    } finally {
      queued = start;
      draining = outer;
      if (start === 0 && COMPLETIONS.length > KEPT_SLOTS) COMPLETIONS.length = 0;
    }
    if (thrown) throw first;
  }

  // Java's complete / completeExceptionally.
  /** @internal */
  override internalComplete(value: T, error: unknown): boolean {
    if (error === null) return this.#settle(SUCCEEDED, value, false);
    return this.#completeWithError(error, false);
  }

  // The record's native future.
  /** @internal */
  override internalSetNative(producer: ProducerHandle, slot: number): void {
    // A delivery recorded first: nothing will wait natively.
    if (this.#state !== PENDING) return;
    this.#native = producer;
    this.#slot = slot;
  }

  // The record's delivery, as its callback saw it.
  /** @internal */
  override internalSetDelivery(value: T, error: unknown): void {
    // JavaScript reads the native future no more: the addon may destroy it.
    this.#native = null;
    this.internalComplete(value, error);
  }

}

// The public class's name, which a message naming an argument's class
// ("got a KafkaFuture") and the console show: the implementation is private.
Object.defineProperty(KafkaFutureImpl, 'name', { value: 'KafkaFuture' });

// Java's nested functional interfaces of KafkaFuture, as function types.
// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested types KafkaFuture.BaseFunction and KafkaFuture.BiConsumer, reached through the class name as in Java; types only, it emits nothing
export declare namespace KafkaFuture {
  /**
   * A function which takes objects of type A and returns objects of type B.
   *
   * @typeParam A - the argument type
   * @typeParam B - the result type
   */
  export type BaseFunction<A, B> = (a: A) => B;
  /**
   * A consumer of two different types of object.
   *
   * @typeParam A - the first argument type
   * @typeParam B - the second argument type
   */
  export type BiConsumer<A, B> = (a: A, b: B) => void;
}
