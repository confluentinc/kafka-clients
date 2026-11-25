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

// Java's Producer interface, with the members KafkaProducer translates
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family). Java's
// Producer has no static member, so it has no runtime object.
//
// Left out, as on KafkaProducer (the producer package's documentation, in
// index.ts): not in the preview, initTransactions, beginTransaction,
// sendOffsetsToTransaction, commitTransaction and abortTransaction; for want
// of an FFI entry point, registerMetricForSubscription,
// unregisterMetricFromSubscription and clientInstanceId.

import type { KafkaFuture } from '../common/kafka_future';
import type { Metric } from '../common/metric';
import type { MetricNameMap } from '../common/metric_name_map';
import type { PartitionInfo } from '../common/partition_info';
import type { Duration } from '../duration';
import type { Callback } from './callback';
import type { CloseArgs } from './kafka_producer';
import type { ProducerRecord } from './producer_record';
import type { RecordMetadata } from './record_metadata';

/**
 * The interface for the `KafkaProducer`.
 *
 * @typeParam K - the type of the keys the producer's key serializer takes
 * @typeParam V - the type of the values the producer's value serializer takes
 *
 * @see KafkaProducer
 */
export interface Producer<K = Uint8Array, V = Uint8Array> {
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
   * @throws `IllegalStateError` rejects with it when the producer is closed,
   *   the core's error with Java's message
   * @throws `IllegalArgumentError` rejects with it when the given parameters
   *   are not `{record}` or `{record, callback}`, or a bare value is not a
   *   ProducerRecord
   * @throws `TypeError` rejects with it when `record` is null or not a
   *   ProducerRecord, `callback` not a function, a parameter is `undefined`,
   *   a serializer returns neither a Uint8Array nor null, a key or value
   *   sent without a serializer is neither bytes nor null (a string
   *   included: it needs `stringSerializer()`), or the
   *   topic or a header key holds a null byte, which the C ABI would
   *   read as the end of the string
   * @throws `InterruptError` rejects with it if the thread is interrupted while
   *   blocked
   * @throws `SerializationError` rejects with it if the key or value are not
   *   valid objects given the configured serializers: a serializer's
   *   `TypeError`, Java's `ClassCastException`, becomes one, "Can't convert
   *   key of class <class> to class <serializer> specified in
   *   key.serializer" (or the value's), the `TypeError` its `cause`
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
   *    Promise is awaited before the future completes. Callbacks run one at a
   *    time, in the order their records complete, as Java's do: a later
   *    record's callback waits until an earlier one's Promise settled. A
   *    future whose `get()` is never called raises no unhandled rejection.
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
   * The serializers run inside the call, key then value, on the record's
   * key and value (null included) with its topic and headers; one returning
   * a Promise is awaited before the hand-over, so sends that are not awaited
   * one by one may then reach the producer out of call order (`await
   * send()` keeps it).
   *
   * What a serializer throws, or its Promise rejects with, is answered as
   * Java's `doSend` answers it. An `ApiError` does not reject: the callback
   * runs once, inside the call, with the error and Java's failure metadata
   * (the record's topic, its partition or -1, and -1 for the offset,
   * timestamp and sizes), and the Promise resolves to a done future failed
   * with it, whose `get()` rejects with an `ExecutionError` around it. What
   * that callback throws, or its Promise rejects with, rejects the send
   * instead, as Java's `onCompletion` throws out of `send()`. A `TypeError`
   * rejects as a `SerializationError` (above); any other error rejects as
   * it is. The message names classes as JavaScript does: a value's
   * constructor (`Buffer`), a primitive's wrapper (`String`), `null` for
   * null, a serializer's class, or a built-in's factory
   * (`stringSerializer`). *(deviation: Java writes fully qualified Java
   * names, and a null key there throws NullPointerException. Why: JavaScript
   * has no class names of that form.)* *(deviation: no failure `send()`
   * reports, a serializer's included, is counted in the producer's
   * `record-error-rate` or `record-error-total`, which Java's `doSend`
   * records for an `ApiException` or a `KafkaException`. Why: the core's
   * `send()` records no error in that sensor, and a serializer's failure is
   * answered in JavaScript, before the core. Goes when: the core records its
   * `send()`'s errors, and, for a serializer's, the FFI can record one: the
   * producer hands out no handle to its `errors` sensor.)*
   *
   * The client reads a `Buffer` or `Uint8Array` key or value after `send()`
   * returns. Don't write to it, transfer it or resize it until the Promise
   * from `send()` settles; awaiting `send()` is enough. A serializer must
   * return a new Buffer for each record. If other workers share the Buffer's
   * memory, they mustn't write to it until then either. What a serializer
   * returns is read the same way, until the Promise from `send()` settles.
   * *(deviation: Java copies the bytes inside `send()`. Why: it saves a
   * memory copy of each Buffer, as the Python binding does.)* Header values
   * are copied during the call. Without a serializer, a key or value is
   * bytes or null: a string rejects with a `TypeError` pointing to
   * `stringSerializer()`. The record's
   * headers become read-only once the serializers have run: a later `add` or
   * `remove` throws `IllegalStateError` "RecordHeaders has been closed.".
   * *(deviation, as in Python: Java's `send()` makes them read-only only
   * after its metadata wait. Why: the serializers run in JavaScript before
   * the record reaches the core.)*
   *
   * Every failure rejects the returned Promise, an argument check's and the
   * closed producer's included: `send()` never throws. *(deviation: on a
   * closed producer the record is checked and converted before the core's
   * closed error, where Java checks the closed producer before it
   * serializes. Why: the binding keeps no state, and the conversion runs in
   * JavaScript before the call reaches the core.)*
   *
   * Inside a callback the core runs in its drain, before its first `await`,
   * `flush()` of the same producer is refused, as Java's throws, and
   * `close()` does not wait: it logs a warning and closes as
   * `close({ timeout: 0 })`, as Java's does, failing the records still
   * pending with "Producer is closed forcefully.". A callback that waited for
   * its turn behind an earlier callback's Promise runs outside the drain, as
   * after its first `await`. After the first `await`, `flush()` and `close()`
   * wait for the callback itself and can hang, and so does awaiting the
   * future of the callback's own record or of a record sent later, which
   * completes only once its callback ran. A callback Promise that never
   * settles has no time limit: it holds its record's future, the callbacks of
   * the records after it, `flush()` and `close()`, `close({ timeout })`
   * included.
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
   * Asynchronously send a record to a topic, Java's `send(record)`: the same
   * as `send({ record, callback: null })`.
   *
   * @param options - the record
   * @returns resolving at hand-over to the future of the record's metadata,
   *   rejecting with the error Java's send() would throw
   */
  send(options: {
    /** The record to send. */
    record: ProducerRecord<K, V>;
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
  send(record: ProducerRecord<K, V>): Promise<KafkaFuture<RecordMetadata>>;

  /**
   * Invoking this method makes all buffered records immediately available to
   * send and resolves once the requests associated with these records
   * complete and their callbacks ran. The records are those whose `send()`
   * had resolved before `flush()` was called: their futures are all done when
   * it resolves.
   *
   * Do not call it from a callback. From a callback that did not wait for its
   * turn, before the callback's first `await`, it rejects, as Java's flush()
   * throws there. After that `await`, or in a callback that waited for its
   * turn behind an earlier callback's result, it is not refused: it waits for
   * that callback, so awaited there it never settles (refusing it there is a
   * planned improvement).
   *
   * @returns resolving once the records sent before it completed
   * @throws `IllegalArgumentError` rejects with it when it is given an
   *   argument: Java's flush() takes none
   * @throws `TypeError` rejects with it when it is given `undefined`
   * @throws `InterruptError` rejects with it if the thread is interrupted while
   *   blocked
   * @throws `KafkaError` rejects with it if the method is invoked inside a
   *   `send` callback that did not wait for its turn, before the callback's
   *   first `await`
   *
   * @remarks A callback that returned a Promise counts as run once it
   * settled, so a callback's Promise must not wait for this flush. On a
   * closed producer it does what the core does.
   */
  flush(): Promise<void>;

  /**
   * Get the partition metadata for the given topic. This can be used for
   * custom partitioning.
   *
   * This will attempt to refresh metadata until it finds the topic in it, or
   * the configured `max.block.ms` expires.
   *
   * @param options - the topic
   * @returns resolving to the topic's partitions, a new array
   * @throws `AuthenticationError` rejects with it if authentication fails.
   *   See the error for more details
   * @throws `AuthorizationError` rejects with it if not authorized to the
   *   specified topic. See the error for more details
   * @throws `InterruptError` rejects with it if the thread is interrupted
   *   while blocked
   * @throws `TimeoutError` rejects with it if the topic cannot be found in
   *   metadata within `max.block.ms`
   * @throws `BootstrapResolutionError` rejects with it if DNS resolution of
   *   the bootstrap servers fails within `bootstrap.resolve.timeout.ms`
   * @throws `KafkaError` rejects with it for all Kafka-related errors,
   *   including the case where this method is called after producer close
   * @throws `IllegalArgumentError` rejects with it when the given parameters
   *   are not `{topic}`
   * @throws `TypeError` rejects with it when `topic` is null ("topic cannot be
   *   null", the core's), `undefined` or not a string
   *
   * @remarks Java's blocking call returns a Promise here, and Java's `List`
   * an array.
   */
  partitionsFor(options: {
    /** The topic to get partition metadata for. */
    topic: string;
  }): Promise<PartitionInfo[]>;
  /**
   * Get the partition metadata for the given topic: the topic on its own,
   * the same as `partitionsFor({topic})`.
   *
   * @param topic - The topic to get partition metadata for
   * @returns resolving to the topic's partitions
   */
  partitionsFor(topic: string): Promise<PartitionInfo[]>;

  /**
   * Get the full set of internal metrics maintained by the producer.
   *
   * @returns the producer's metrics, by name
   * @throws `TypeError` when it is given an argument
   *
   * The returned map is an unmodifiable live view of the metrics. Changes to
   * the underlying metrics will be reflected in the returned map.
   *
   * @remarks A `MetricNameMap` of `KafkaMetric`s, read when asked: its keys
   * and size at each call, and each metric's value at each `metricValue()`.
   * It answers after `close()` too.
   */
  metrics(): MetricNameMap<Metric>;

  /**
   * Close this producer. This method blocks until all previously sent
   * requests complete. This method is equivalent to
   * `close({ timeout: Infinity })`.
   *
   * **If `close()` is called from a `Callback`, a warning message will be
   * logged and `close({ timeout: 0 })` will be called instead.** We do this
   * because the producer would otherwise wait for the callback it is called
   * from and block forever.
   *
   * @returns resolving once the producer is closed
   * @throws `InterruptError` rejects with it if the thread is interrupted while
   *   blocked
   * @throws `KafkaError` rejects with it if an unexpected error occurs while
   *   trying to close the client; this error should be treated as fatal and
   *   indicate the client is no longer usable
   * @throws `TypeError` rejects with it when it is given `undefined`
   *
   * @remarks It waits for the records whose `send()` had resolved, and their
   * callbacks' Promises; it never throws. Close every producer: one keeps Node
   * running until closed. After its first `await`, `close()` in a callback
   * waits for it.
   */
  close(): Promise<void>;
  /**
   * This method waits up to `timeout` for the producer to complete the
   * sending of all incomplete requests. If the producer is unable to complete
   * all requests before the timeout expires, this method will fail any unsent
   * and unacknowledged records immediately. A timeout of zero means do not
   * wait for pending send requests to complete.
   *
   * If invoked from within a `Callback` this method will not block and will
   * be equivalent to `close({ timeout: 0 })`. This is done since no further
   * sending will happen while the callback blocks the producer.
   *
   * @param options - the timeout
   * @returns resolving once the producer is closed
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
   * @remarks A callback Promise that never settles holds the close past its
   * timeout. Called from a callback the core runs in its
   * drain, before its first `await`, it is `close({ timeout: 0 })` and waits
   * for no callback's Promise; after that `await`, or from a callback that
   * waited for its turn behind an earlier callback's Promise, it waits for the
   * callback it is called from, so a callback that awaits it never settles.
   */
  close(options: CloseArgs): Promise<void>;
  /**
   * Close this producer, waiting up to `timeout` for the pending requests:
   * the timeout on its own, the same as `close({timeout})`, Java's
   * close(Duration).
   *
   * @param timeout - The maximum time to wait for the producer to complete
   *   any pending requests. A timeout of zero means do not wait.
   * @returns resolving once the producer is closed
   */
  close(timeout: Duration): Promise<void>;

  /**
   * Closes the producer as `close()` does, so that `await using` closes it.
   *
   * @returns resolving once the producer is closed, rejecting as `close()`
   *   rejects
   *
   * @remarks *(js-only)* JavaScript's async disposal protocol, the form
   * Java's `Closeable.close()` takes in a `try`-with-resources block.
   */
  [Symbol.asyncDispose](): Promise<void>;
}
