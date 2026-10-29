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
const { illegalArgument, illegalState } = require('../common/kafka_error');
const { KafkaFuture } = require('../common/kafka_future');
const { checkCallback, thrownMessage } = require('./callback');
const { ProducerRecord } = require('./producer_record');

// Java's KafkaProducer.throwIfProducerClosed message.
const CLOSED_MESSAGE = 'Cannot perform operation after producer has been closed';
// Java's KafkaProducer.close(Duration) message for a negative timeout.
const NEGATIVE_TIMEOUT_MESSAGE = 'The timeout cannot be negative.';

// A send() the producer takes at once settles its Promise inside the call, so
// awaiting it runs only microtasks: a loop that awaits such sends never returns
// to the event loop, where timers, I/O and the threadsafe function's callback
// drain run, and every record's delivery state piles up until it ends. After
// this many such sends in a row, the next one's Promise settles from
// setImmediate, giving the event loop one turn (Python's
// AsyncProducer._FAST_PATH_YIELD_EVERY). Its record is handed over at once all
// the same: only the resolution waits.
const FAST_PATH_YIELD_EVERY = 1000;
// Between those turns, every this many such sends in a row (and on the first
// after a queued hand-over) send() runs the callback drain itself on entry, so
// delivery callbacks run and their state is released in batches of at most
// this many records (Python's AsyncProducer._PUMP_EVERY).
const PUMP_EVERY = 64;

/**
 * A Kafka client that publishes records to the Kafka cluster.
 *
 * The producer is thread safe in Java; here every call is made from the JS
 * thread, and the delivery callbacks run there too, from the event loop.
 * Records are batched and sent by the client in the background; `send`
 * resolves once the record has reached the producer, with a future of the
 * record's metadata.
 */
class KafkaProducer {
  #handle;
  // close() was called: send refuses records from then on.
  #closed = false;
  // The native producer was destroyed by a completed close.
  #destroyed = false;
  #closing = null;
  // Sends in a row whose hand-over completed inside the call.
  #fastPathSends = 0;

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
   * run: after 1000 such sends in a row, the next one resolves only after one
   * event-loop turn (its record is handed over at once all the same), and
   * every 64 `send()` runs the delivery callbacks already due before handing
   * its record over.
   *
   * A Buffer or Uint8Array key or value is borrowed, not copied, until the
   * returned Promise settles: it must not be modified or detached (transferred)
   * before then, and may be reused after. A string key or value is encoded
   * into a Buffer of the client's own and is unaffected.
   *
   * @param {ProducerRecord} record The record to send
   * @param {((metadata: RecordMetadata, error: KafkaError | null) => void) | null} [callback] A
   *   user-supplied callback to execute when the record has been acknowledged by the server
   * @returns {Promise<KafkaFuture<RecordMetadata>>} resolving at hand-over to the future of the
   *   record's metadata, rejecting with the KafkaError Java's send() would throw
   * @throws {KafkaError} `LOCAL_ILLEGAL_STATE` when the producer is closed
   * @throws {TypeError} when `record` is not a ProducerRecord or `callback` not a function
   */
  send(record, callback) {
    if (!(record instanceof ProducerRecord)) throw new TypeError('record must be a ProducerRecord');
    const userCallback = checkCallback(callback);
    if (this.#closed) throw illegalState(CLOSED_MESSAGE);
    if (this.#fastPathSends % PUMP_EVERY === 0) {
      // A no-op inside a drain (a send from a delivery callback).
      native.producerPump(this.#handle);
      // A callback it ran may have closed the producer.
      if (this.#closed) throw illegalState(CLOSED_MESSAGE);
    }
    const yieldTurn = this.#fastPathSends >= FAST_PATH_YIELD_EVERY;
    if (yieldTurn) this.#fastPathSends = 0;
    const args = record._toNative();
    const future = KafkaFuture._create();
    let resolve;
    let reject;
    const handedOver = new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    });
    // The hand-over, inside producerSend for a record taken or refused at once,
    // later otherwise: an error is the one Java's send() would throw. Never
    // marked handled, so an un-awaited send that fails surfaces.
    let inCall = true;
    const settle = (error) => {
      if (error !== null) reject(error);
      else resolve(future);
    };
    const onHandover = (error) => {
      if (!inCall) {
        // Queued: the caller's await already gave the event loop its turns.
        this.#fastPathSends = 0;
        settle(error);
      } else if (yieldTurn) {
        setImmediate(settle, error);
      } else {
        this.#fastPathSends++;
        settle(error);
      }
    };
    // The record's delivery callback: the user callback first, then the
    // future; a message returned is what the callback threw, for the core to log.
    const onComplete = (metadata, error) => {
      let failure;
      if (userCallback !== null) {
        try {
          userCallback(metadata, error);
        } catch (thrown) {
          failure = thrownMessage(thrown);
        }
      }
      future._complete(metadata, error);
      return failure;
    };
    try {
      native.producerSend(this.#handle, ...args, onHandover, onComplete);
    } finally {
      inCall = false;
    }
    return handedOver;
  }

  /**
   * Invoking this method makes all buffered records immediately available to
   * send and resolves once the requests associated with these records
   * complete and their callbacks ran. The records are those whose `send()`
   * had resolved before `flush()` was called: their futures are all done when
   * it resolves.
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
   * close(Duration)), then releases the native client. The records waited
   * for are those whose `send()` had resolved before `close()` was called.
   * Calling it again returns the close in progress, or resolves once the
   * producer is closed. A negative `timeoutMs` is rejected whatever the
   * producer's state and changes nothing, as Java's close(Duration) checks it
   * first.
   *
   * @param {{timeoutMs?: number}} [options] `timeoutMs`: the maximum time to wait for the
   *   producer to complete any pending requests. A timeout of zero means do not wait.
   * @returns {Promise<void>} rejecting with `LOCAL_ILLEGAL_ARGUMENT` for a negative `timeoutMs`
   * @throws {TypeError} when `timeoutMs` is not an integer
   */
  close(options) {
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
    // Java's close(Duration) rejects a negative timeout before anything else,
    // whatever the state. The core checks it too, but its verdict arrives at
    // the next drain, after this call marked the producer closed: checked here,
    // a rejected close changes nothing (javascript/CLAUDE.md, rule 1).
    if (timeoutMs !== null && timeoutMs < 0) return Promise.reject(illegalArgument(NEGATIVE_TIMEOUT_MESSAGE));
    if (this.#closing !== null || this.#destroyed) {
      return this.#closing !== null ? this.#closing : Promise.resolve();
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
