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


// Type declarations of the Node.js client (producer slice of Java's API).

/// <reference types="node" />

/** A key, value or header value: bytes, or a string encoded as UTF-8. */
export type Data = Buffer | Uint8Array | string;

/** A record header, Java's Header. */
export interface Header {
  readonly key: string;
  readonly value: Data | null;
}

/**
 * An error from the Kafka client: every error the C FFI reports.
 */
export class KafkaError extends Error {
  /** The `kafka_common_Error_e` value identifying the error's class. */
  readonly code: number;
  /** The enumerator name of `code` (`TOPIC_AUTHORIZATION_FAILED`, `LOCAL_ILLEGAL_STATE`, ...). */
  readonly name: string;
  /** Java's `instanceof RetriableException`. */
  readonly retriable: boolean;
  /** Java's `instanceof ApiException`. */
  readonly apiError: boolean;
  /** The error's cause, Java's `getCause()`. */
  readonly cause?: unknown;
  private constructor(fields: never);
}

/** Options of {@link ProducerRecord}'s constructor: Java's overloaded constructors in one. */
export interface ProducerRecordOptions {
  /** The topic the record will be appended to. */
  topic: string;
  /** The partition to which the record should be sent. */
  partition?: number | null;
  /** The timestamp of the record, in milliseconds since epoch; null: the producer's current time. */
  timestamp?: number | null;
  /** The key that will be included in the record. */
  key?: Data | null;
  /** The record contents. */
  value?: Data | null;
  /** The headers that will be included in the record. */
  headers?: Iterable<Header> | null;
}

/** A key/value pair to be sent to Kafka. */
export class ProducerRecord {
  /**
   * @throws {KafkaError} `LOCAL_ILLEGAL_ARGUMENT` when the topic is null, or the timestamp or
   *   the partition is negative
   * @throws {TypeError} when an argument has the wrong type
   */
  constructor(options: ProducerRecordOptions);
  /** The topic this record is being sent to. */
  readonly topic: string;
  /** The headers. */
  readonly headers: ReadonlyArray<Header>;
  /** The key (or null if no key is specified). */
  readonly key: Data | null;
  /** The value. */
  readonly value: Data | null;
  /** The timestamp, which is in milliseconds since epoch. */
  readonly timestamp: number | null;
  /** The partition to which the record will be sent (or null if no partition was specified). */
  readonly partition: number | null;
  toString(): string;
}

/** The metadata for a record that has been acknowledged by the server. */
export class RecordMetadata {
  /** Partition value for record without partition assigned. */
  static readonly UNKNOWN_PARTITION: -1;
  private constructor(fields: never);
  /** true if the offset is included in the metadata, false otherwise. */
  readonly hasOffset: boolean;
  /** The offset of the record in the topic/partition, or -1 if hasOffset is false. */
  readonly offset: number;
  /** true if a valid timestamp exists, false otherwise. */
  readonly hasTimestamp: boolean;
  /** The timestamp of the record in the topic/partition, or -1 if hasTimestamp is false. */
  readonly timestamp: number;
  /** The size of the serialized, uncompressed key in bytes, -1 for a null key. */
  readonly serializedKeySize: number;
  /** The size of the serialized, uncompressed value in bytes, -1 for a null value. */
  readonly serializedValueSize: number;
  /** The topic the record was appended to. */
  readonly topic: string;
  /** The partition the record was sent to. */
  readonly partition: number;
  toString(): string;
}

/**
 * Java's Callback: run when the record sent to the server has been acknowledged. When `error`
 * is not null, metadata contains the special -1 value for all fields but the topic.
 */
export type Callback = (metadata: RecordMetadata, error: KafkaError | null) => void;

/**
 * A future result of an asynchronous operation, Java's `KafkaFuture`; the one
 * {@link KafkaProducer.send} resolves to is Java's `FutureRecordMetadata`. A
 * failed future rejects only the Promises `get()` returned: one whose `get()`
 * is never called raises no unhandled rejection.
 */
export class KafkaFuture<T> {
  private constructor(token: never);
  /** Waits if necessary for this future to complete, then resolves to its result or rejects with its error. */
  get(): Promise<T>;
  /** true if completed in any fashion: normally, exceptionally, or via cancellation. */
  isDone(): boolean;
  /** true if completed exceptionally. */
  isCompletedExceptionally(): boolean;
  /** false: a record's future is never cancelled. */
  isCancelled(): boolean;
  /** Java's `FutureRecordMetadata.cancel`: cancels nothing and returns false. */
  cancel(mayInterruptIfRunning?: boolean): false;
}

/** A producer's configuration: Java's ProducerConfig keys. */
export type ProducerConfigs =
  | Record<string, string | number | boolean | bigint | ReadonlyArray<string | number> | null | undefined>
  | Map<string, unknown>;

/** Options of {@link KafkaProducer.close}. */
export interface CloseOptions {
  /** The maximum time to wait for the producer to complete any pending requests, in milliseconds. */
  timeoutMs?: number;
}

/** A Kafka client that publishes records to the Kafka cluster. */
export class KafkaProducer {
  /** @throws {KafkaError} when the configuration is invalid or the producer cannot be built */
  constructor(configs: ProducerConfigs);
  /**
   * Asynchronously send a record to a topic, in two steps. The returned Promise resolves once
   * the record has been handed over to the producer (where Java's blocking `send()` returns:
   * at once, or after up to `max.block.ms` waiting for metadata or buffer memory), with the
   * future of its metadata; it rejects with the error Java's `send()` would throw, which never
   * reaches the callback. The future's `get()` settles at delivery; `callback` runs first.
   *
   * Ordering is the caller's: records from code that awaits each `send()` before the next reach
   * the producer in that order and are covered by a following `flush()` / `close()`. Sends not
   * awaited one by one (e.g. under `Promise.all`) are concurrent, like Java `send()` calls on
   * several threads, with no order between them. A loop awaiting sends taken at once still lets
   * the event loop run: after 1000 such sends in a row, such sends resolve only after one
   * event-loop turn, whichever of them is awaited (their records are handed over at once all the
   * same), and every 64th runs the delivery callbacks already due, inside the `send()` call.
   *
   * A `Buffer` / `Uint8Array` key or value is borrowed, not copied, until the returned Promise
   * settles: do not modify or detach it before then; it may be reused after. A string key or
   * value is encoded into a Buffer of the client's own and is unaffected.
   *
   * @throws {KafkaError} `LOCAL_ILLEGAL_STATE` when the producer is closed
   */
  send(record: ProducerRecord, callback?: Callback | null): Promise<KafkaFuture<RecordMetadata>>;
  /**
   * Makes all buffered records immediately available to send and waits for them to complete:
   * the records whose `send()` had resolved before, whose futures are all done when it resolves.
   */
  flush(): Promise<void>;
  /** Close this producer, waiting for previously sent requests (up to `timeoutMs` when given). */
  close(options?: CloseOptions): Promise<void>;
}
