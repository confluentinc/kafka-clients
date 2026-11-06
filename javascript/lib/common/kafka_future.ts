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

// Java's KafkaFuture: the future a send hands back once the record has reached
// the producer, completed by the record's delivery.

const PENDING = 0;
const SUCCEEDED = 1;
const FAILED = 2;

// Only the client makes futures.
const CONSTRUCT = Symbol('KafkaFuture');

/**
 * A future result of an asynchronous operation, Java's `KafkaFuture`; the one
 * `KafkaProducer.send` resolves to is Java's `FutureRecordMetadata`.
 *
 * `get()` returns a Promise that settles once the operation completes. A
 * failed future rejects only the Promises `get()` returned: a future whose
 * `get()` is never called raises no unhandled rejection, as Java ignores an
 * unread failed `Future`. It cannot be cancelled: `cancel()` returns false.
 *
 * @typeParam T - the result type
 */
export class KafkaFuture<T> {
  #state: number = PENDING;
  #value: T | undefined;
  #error: unknown;
  // The settle functions of the Promises get() returned while pending:
  // resolve and reject of each, in turn.
  #waiters: Array<(value: never) => void> | null = null;

  /**
   * Futures are made by the client (private).
   */
  private constructor(token: symbol) {
    if (token !== CONSTRUCT) throw new TypeError('KafkaFuture is not constructible');
  }

  /**
   * Waits if necessary for this future to complete, and then returns its
   * result.
   *
   * @returns resolving to the result value, rejecting with the error the
   *   future failed with
   */
  get(): Promise<T> {
    if (this.#state === SUCCEEDED) return Promise.resolve(this.#value as T);
    if (this.#state === FAILED) return Promise.reject(this.#error);
    return new Promise<T>((resolve, reject) => {
      if (this.#waiters === null) this.#waiters = [];
      this.#waiters.push(resolve, reject);
    });
  }

  /**
   * Returns true if completed in any fashion: normally, exceptionally, or via cancellation.
   */
  isDone(): boolean {
    return this.#state !== PENDING;
  }

  /**
   * Returns true if completed exceptionally.
   */
  isCompletedExceptionally(): boolean {
    return this.#state === FAILED;
  }

  /**
   * Returns false: a record's future is never cancelled.
   */
  isCancelled(): boolean {
    return false;
  }

  /**
   * Java's `FutureRecordMetadata.cancel`: the record is not cancelled.
   */
  cancel(mayInterruptIfRunning?: boolean): false {
    void mayInterruptIfRunning;
    return false;
  }

  /**
   * Completes the future (private): `error` null for success with `value`.
   * Only the first completion counts. Called by `KafkaProducer` (through
   * `future['_complete']`, which TypeScript's `private` allows).
   */
  private _complete(value: T, error: unknown): void {
    if (this.#state !== PENDING) return;
    const waiters = this.#waiters;
    this.#waiters = null;
    if (error === null) {
      this.#state = SUCCEEDED;
      this.#value = value;
    } else {
      this.#state = FAILED;
      this.#error = error;
    }
    if (waiters === null) return;
    for (let i = 0; i < waiters.length; i += 2) {
      if (error === null) (waiters[i] as (value: T) => void)(value);
      else (waiters[i + 1] as (error: unknown) => void)(error);
    }
  }

  /**
   * A pending future (private). Called by `KafkaProducer` (through
   * `KafkaFuture['_create']`, which TypeScript's `private` allows).
   */
  private static _create<T>(): KafkaFuture<T> {
    return new KafkaFuture<T>(CONSTRUCT);
  }
}
