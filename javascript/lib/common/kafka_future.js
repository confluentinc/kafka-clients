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


'use strict';

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
 * @template T
 */
class KafkaFuture {
  #state = PENDING;
  #value;
  #error;
  // The settle functions of the Promises get() returned while pending.
  #waiters = null;

  /** Futures are made by the client (private). */
  constructor(token) {
    if (token !== CONSTRUCT) throw new TypeError('KafkaFuture is not constructible');
  }

  /**
   * Waits if necessary for this future to complete, and then returns its
   * result.
   *
   * @returns {Promise<T>} resolving to the result value, rejecting with the
   *   error the future failed with
   */
  get() {
    if (this.#state === SUCCEEDED) return Promise.resolve(this.#value);
    if (this.#state === FAILED) return Promise.reject(this.#error);
    return new Promise((resolve, reject) => {
      if (this.#waiters === null) this.#waiters = [];
      this.#waiters.push(resolve, reject);
    });
  }

  /** @returns {boolean} true if completed in any fashion: normally, exceptionally, or via cancellation */
  isDone() {
    return this.#state !== PENDING;
  }

  /** @returns {boolean} true if completed exceptionally */
  isCompletedExceptionally() {
    return this.#state === FAILED;
  }

  /** @returns {boolean} false: a record's future is never cancelled */
  isCancelled() {
    return false;
  }

  /**
   * Java's `FutureRecordMetadata.cancel`: the record is not cancelled.
   *
   * @param {boolean} [mayInterruptIfRunning]
   * @returns {boolean} false
   */
  cancel(mayInterruptIfRunning) {
    void mayInterruptIfRunning;
    return false;
  }

  /**
   * Completes the future (private): `error` null for success with `value`.
   * Only the first completion counts.
   */
  _complete(value, error) {
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
      if (error === null) waiters[i](value);
      else waiters[i + 1](error);
    }
  }

  /** A pending future (private). */
  static _create() {
    return new KafkaFuture(CONSTRUCT);
  }
}

module.exports = { KafkaFuture };
