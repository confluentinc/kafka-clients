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

// Java's KafkaProducer over the C FFI's KafkaProducer with NULL serializers:
// keys and values cross as bytes (javascript/CLAUDE.md).

const native = require('../_native');
const { configEntries } = require('../_config');
const { illegalState } = require('../common/kafka_error');
const { checkCallback, thrownMessage } = require('./callback');
const { ProducerRecord } = require('./producer_record');

// Java's KafkaProducer.throwIfProducerClosed message.
const CLOSED_MESSAGE = 'Cannot perform operation after producer has been closed';

/**
 * A Kafka client that publishes records to the Kafka cluster.
 *
 * The producer is thread safe in Java; here every call is made from the JS
 * thread, and the delivery callbacks run there too, from the event loop.
 * Records are batched and sent by the client in the background; `send`
 * returns a Promise of the record's metadata.
 */
class KafkaProducer {
  #handle;
  // close() was called: send refuses records from then on.
  #closed = false;
  // The native producer was destroyed by a completed close.
  #destroyed = false;
  #closing = null;

  /**
   * A producer is instantiated by providing a set of key-value pairs as
   * configuration. Valid configuration strings are documented in Java's
   * ProducerConfig.
   *
   * @param {Record<string, unknown> | Map<string, unknown>} configs The producer configs
   * @throws {KafkaError} when the configuration is invalid or the producer cannot be built
   */
  constructor(configs) {
    this.#handle = native.producerCreate(configEntries(configs));
  }

  /**
   * Asynchronously send a record to a topic and invoke the provided callback
   * when the send has been acknowledged.
   *
   * The send is asynchronous and this method will return immediately (once
   * the record has been handed to the producer) with a Promise of the
   * RecordMetadata assigned to the record. The callback, when given, runs
   * before the Promise settles, on the JS thread.
   *
   * @param {ProducerRecord} record The record to send
   * @param {((metadata: RecordMetadata, error: KafkaError | null) => void) | null} [callback] A
   *   user-supplied callback to execute when the record has been acknowledged by the server
   * @returns {Promise<RecordMetadata>} resolving to the record's metadata, rejecting with the
   *   KafkaError the send or the delivery failed with
   * @throws {KafkaError} `LOCAL_ILLEGAL_STATE` when the producer is closed
   * @throws {TypeError} when `record` is not a ProducerRecord or `callback` not a function
   */
  send(record, callback) {
    if (!(record instanceof ProducerRecord)) throw new TypeError('record must be a ProducerRecord');
    const userCallback = checkCallback(callback);
    if (this.#closed) throw illegalState(CLOSED_MESSAGE);
    const args = record._toNative();
    let resolve;
    let reject;
    const promise = new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    });
    let settled = false;
    // The hand-over: an error is the one Java's send() would throw.
    const onHandover = (error) => {
      if (error !== null && !settled) {
        settled = true;
        reject(error);
      }
    };
    // The record's delivery callback: the user callback first, then the
    // Promise; a message returned is what the callback threw, for the core to log.
    const onComplete = (metadata, error) => {
      let failure;
      if (userCallback !== null) {
        try {
          userCallback(metadata, error);
        } catch (thrown) {
          failure = thrownMessage(thrown);
        }
      }
      if (!settled) {
        settled = true;
        if (error !== null) reject(error);
        else resolve(metadata);
      }
      return failure;
    };
    native.producerSend(this.#handle, ...args, onHandover, onComplete);
    // A send with a callback is commonly not awaited: its failure reached the
    // callback, so it must not also surface as an unhandled rejection.
    if (userCallback !== null) promise.catch(() => {});
    return promise;
  }

  /**
   * Invoking this method makes all buffered records immediately available to
   * send and resolves once the requests associated with these records
   * complete and their callbacks ran.
   *
   * Calling it from inside a callback rejects, as Java's flush() throws there.
   *
   * @returns {Promise<void>}
   */
  flush() {
    if (this.#destroyed) {
      // A completed close completed every record: Java's flush() then returns
      // at once, the accumulator being empty.
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      native.producerFlush(this.#handle, (error) => (error !== null ? reject(error) : resolve()));
    });
  }

  /**
   * Close this producer. Waits until all previously sent requests complete
   * (`close()`), or up to `timeoutMs` for them (`close({timeoutMs})`, Java's
   * close(Duration)), then releases the native client. Calling it again
   * returns the close in progress, or resolves once the producer is closed.
   *
   * @param {{timeoutMs?: number}} [options] `timeoutMs`: the maximum time to wait for the
   *   producer to complete any pending requests. A timeout of zero means do not wait.
   * @returns {Promise<void>}
   * @throws {TypeError} when `timeoutMs` is not an integer
   */
  close(options) {
    if (this.#closing !== null) return this.#closing;
    if (this.#destroyed) return Promise.resolve();
    let timeoutMs = null;
    if (options !== undefined && options !== null) {
      if (typeof options !== 'object') throw new TypeError('close takes an options object {timeoutMs}');
      if (options.timeoutMs !== undefined && options.timeoutMs !== null) {
        if (typeof options.timeoutMs !== 'number' || !Number.isSafeInteger(options.timeoutMs)) {
          throw new TypeError('timeoutMs must be an integer');
        }
        timeoutMs = options.timeoutMs;
      }
    }
    this.#closed = true;
    this.#closing = new Promise((resolve, reject) => {
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

module.exports = { KafkaProducer };
