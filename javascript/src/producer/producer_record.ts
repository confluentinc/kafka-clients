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

// `Data` names Node's Buffer: the directive carries @types/node into the
// compiled producer_record.d.ts (`preserve`, as tsc >= 5.5 drops it from
// declarations otherwise), so a project whose tsconfig restricts `types`
// still resolves it.
/// <reference types="node" preserve="true" />

// Java's ProducerRecord<K, V> over bytes: a key and a value are a Buffer, a
// Uint8Array, a string (encoded as UTF-8 at send, as Java's StringSerializer
// encodes it: src/internal/encode.ts) or null. The headers are a Headers
// (common/header), sealed by send().

import { JavaForms } from '../internal/args';
import { bytesToString } from '../internal/bytes';
import { encodeLikeJava } from '../internal/encode';
import { illegalArgument } from '../internal/errors';
import type { Header } from '../common/header/header';
import { Headers } from '../common/header/headers';

/**
 * A key or value: bytes, or a string encoded as UTF-8.
 *
 * @remarks *(js-only)* The JavaScript types a key or value takes; Java's are
 * the serializers' generic types.
 */
export type Data = Buffer | Uint8Array | string;

// Java's constructors, in Java declaration order (src/internal/args.ts).
const FORMS = new JavaForms(
  'ProducerRecord',
  [
    ['topic', 'partition', 'timestamp', 'key', 'value', 'headers'],
    ['topic', 'partition', 'timestamp', 'key', 'value'],
    ['topic', 'partition', 'key', 'value', 'headers'],
    ['topic', 'partition', 'key', 'value'],
    ['topic', 'key', 'value'],
    ['topic', 'value'],
  ],
  { undefinedMessages: { value: "value must not be undefined. Pass null for Java's null, which sends a tombstone." } },
);

/** A header as the native send takes it: key and value bytes. */
type NativeHeader = [Uint8Array, Uint8Array | null];

/**
 * The record arguments of the native send (`internal/native.ts`
 * `producerSend`): topic, partition, timestamp, key, value, which of key (1)
 * and value (2) are the client's own bytes (an encoded string, which the
 * addon need not copy), headers.
 */
type NativeRecord = [string, number, number, Uint8Array | null, Uint8Array | null, number, NativeHeader[] | null];

const INT32_MAX = 2 ** 31 - 1;

function isBytes(value: unknown): value is Uint8Array {
  return value instanceof Uint8Array;
}

function checkData(value: unknown, what: string): Data | null {
  if (value === undefined || value === null) return null;
  if (typeof value === 'string' || isBytes(value)) return value;
  throw new TypeError(`${what} must be a Buffer, a Uint8Array, a string or null`);
}

function toBytes(value: Data | null): Uint8Array | null {
  if (value === null) return null;
  return typeof value === 'string' ? encodeLikeJava(value) : value;
}

/** Which of `key` (1) and `value` (2) are strings, so their bytes are the client's own. */
function ownedBytes(key: Data | null, value: Data | null): number {
  return (typeof key === 'string' ? 1 : 0) | (typeof value === 'string' ? 2 : 0);
}

// Java's `new RecordHeaders(headers)`: new headers holding the given ones.
function checkHeaders(headers: unknown): Headers {
  return Headers.internalTaken(headers === undefined ? null : headers);
}

// Java's Integer / Long argument: null, or an integer number.
function checkOptionalInteger(value: unknown, what: string, max: number): number | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError(`${what} must be an integer or null`);
  if (value > max) throw new RangeError(`${what} must be at most ${max}`);
  return value;
}

// The constructor's checks of one argument, shared with the send-time
// re-check (`internalToNative`), so both throw the same errors.

function checkTopic(topic: unknown): string {
  if (topic === undefined || topic === null) throw illegalArgument('Topic cannot be null.');
  if (typeof topic !== 'string') throw new TypeError('topic must be a string');
  return topic;
}

function checkTimestamp(timestamp: unknown): number | null {
  const checked = checkOptionalInteger(timestamp, 'timestamp', Number.MAX_SAFE_INTEGER);
  if (checked !== null && checked < 0) {
    throw illegalArgument(`Invalid timestamp: ${checked}. Timestamp should always be non-negative or null.`);
  }
  return checked;
}

function checkPartition(partition: unknown): number | null {
  const checked = checkOptionalInteger(partition, 'partition', INT32_MAX);
  if (checked !== null && checked < 0) {
    throw illegalArgument(`Invalid partition: ${checked}. Partition number should always be non-negative or null.`);
  }
  return checked;
}

const VALUE_UNDEFINED = "value must not be undefined. Pass null for Java's null, which sends a tombstone.";

// The constructor's message for a parameter given as `undefined`
// (src/internal/args.ts), for a field reassigned to `undefined`.
function undefinedField(name: string): TypeError {
  return new TypeError(
    name === 'value' ? VALUE_UNDEFINED : `${name} must not be undefined. Leave the key out, or pass null for Java's null`,
  );
}

// A field read at send: `undefined` is refused, as the constructor refuses a
// parameter given as `undefined` (it reads one left out as `null`, which a
// field cannot be).
function defined(value: unknown, name: string): unknown {
  if (value === undefined) throw undefinedField(name);
  return value;
}

/**
 * A key/value pair to be sent to Kafka. This consists of a topic name to which
 * the record is being sent, an optional partition number, and an optional key
 * and value.
 *
 * If a valid partition number is specified that partition will be used when
 * sending the record. If no partition is specified but a key is present a
 * partition will be chosen using a hash of the key. If neither key nor
 * partition is present a partition will be assigned in a round-robin fashion.
 *
 * The record also has an associated timestamp. If the user did not provide a
 * timestamp, the producer will stamp the record with its current time.
 */
export class ProducerRecord {
  /** The topic this record is being sent to */
  readonly topic: string;

  /** The partition to which the record will be sent (or null if no partition was specified) */
  readonly partition: number | null;

  /** The timestamp, which is in milliseconds since epoch. */
  readonly timestamp: number | null;

  /** The key (or null if no key is specified) */
  readonly key: Data | null;

  /** The value */
  readonly value: Data | null;

  /** The headers: the record's own, which `send()` makes read-only */
  readonly headers: Headers;

  /**
   * Creates a record with a specified timestamp to be sent to a specified
   * topic and partition.
   *
   * @param options - the record's topic, partition, timestamp, key, value
   *   and headers
   * @throws `IllegalArgumentError` when the given parameters are not one of
   *   Java's constructors, or the topic is null, or the timestamp or the
   *   partition is negative, as Java's IllegalArgumentException
   * @throws `TypeError` when an argument has the wrong type or is `undefined`
   * @throws `RangeError` when the partition is above 2^31 - 1 or the timestamp
   *   above 2^53 - 1, beyond Java's `Integer` and the integers a JS number
   *   holds exactly
   *
   * @remarks The parameters given are one of Java's constructors, each a
   * signature of its own: `{topic, partition, timestamp, key, value,
   * headers}`, `{topic, partition, timestamp, key, value}`, `{topic,
   * partition, key, value, headers}`, `{topic, partition, key, value}`,
   * `{topic, key, value}` or `{topic, value}`; one a constructor leaves out is
   * what it passes (`null`, no headers). `null` is Java's `null`; a key set to
   * `undefined` is refused (leave the key out). The fields are `readonly` but
   * not frozen: `send()` checks them again.
   */
  constructor(options: {
    /** The topic the record will be appended to. */
    topic: string;
    /** The partition to which the record should be sent, or null. */
    partition: number | null;
    /**
     * The timestamp of the record, in milliseconds since epoch. If null, the
     * producer will assign the timestamp using System.currentTimeMillis().
     */
    timestamp: number | null;
    /** The key that will be included in the record, or null. */
    key: Data | null;
    /** The record contents, or null (a tombstone). */
    value: Data | null;
    /**
     * The headers that will be included in the record: a `Headers` or an
     * iterable of `Header`, copied into the record's own `Headers` (the
     * `Header` objects are kept), or null.
     */
    headers: Iterable<Header> | null;
  });
  /**
   * Creates a record with the specified timestamp to be sent to a specified
   * topic and partition, Java's `ProducerRecord(topic, partition, timestamp,
   * key, value)`: no headers.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic the record will be appended to. */
    topic: string;
    /** The partition to which the record should be sent, or null. */
    partition: number | null;
    /**
     * The timestamp of the record, in milliseconds since epoch. If null, the
     * producer will assign the timestamp using System.currentTimeMillis().
     */
    timestamp: number | null;
    /** The key that will be included in the record, or null. */
    key: Data | null;
    /** The record contents, or null (a tombstone). */
    value: Data | null;
    headers?: never;
  });
  /**
   * Creates a record to be sent to a specified topic and partition, Java's
   * `ProducerRecord(topic, partition, key, value, headers)`: no timestamp.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic the record will be appended to. */
    topic: string;
    /** The partition to which the record should be sent, or null. */
    partition: number | null;
    timestamp?: never;
    /** The key that will be included in the record, or null. */
    key: Data | null;
    /** The record contents, or null (a tombstone). */
    value: Data | null;
    /**
     * The headers that will be included in the record: a `Headers` or an
     * iterable of `Header`, copied into the record's own `Headers` (the
     * `Header` objects are kept), or null.
     */
    headers: Iterable<Header> | null;
  });
  /**
   * Creates a record to be sent to a specified topic and partition, Java's
   * `ProducerRecord(topic, partition, key, value)`: no timestamp, no headers.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic the record will be appended to. */
    topic: string;
    /** The partition to which the record should be sent, or null. */
    partition: number | null;
    timestamp?: never;
    /** The key that will be included in the record, or null. */
    key: Data | null;
    /** The record contents, or null (a tombstone). */
    value: Data | null;
    headers?: never;
  });
  /**
   * Create a record to be sent to Kafka, Java's `ProducerRecord(topic, key,
   * value)`: no partition, timestamp or headers.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic the record will be appended to. */
    topic: string;
    partition?: never;
    timestamp?: never;
    /** The key that will be included in the record, or null. */
    key: Data | null;
    /** The record contents, or null (a tombstone). */
    value: Data | null;
    headers?: never;
  });
  /**
   * Create a record with no key, Java's `ProducerRecord(topic, value)`.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic the record will be appended to. */
    topic: string;
    partition?: never;
    timestamp?: never;
    key?: never;
    /** The record contents, or null (a tombstone). */
    value: Data | null;
    headers?: never;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const topic = FORMS.value(options, 'topic');
    const partition = FORMS.value(options, 'partition');
    const timestamp = FORMS.value(options, 'timestamp');
    const key = FORMS.value(options, 'key');
    const value = FORMS.value(options, 'value');
    const headers = FORMS.value(options, 'headers');
    const checkedTopic = checkTopic(topic);
    const checkedTimestamp = checkTimestamp(timestamp);
    const checkedPartition = checkPartition(partition);
    this.topic = checkedTopic;
    this.partition = checkedPartition;
    this.timestamp = checkedTimestamp;
    this.key = checkData(key, 'key');
    this.value = checkData(value, 'value');
    this.headers = checkHeaders(headers);
  }

  toString(): string {
    return (
      `ProducerRecord(topic=${this.topic}, partition=${this.partition}, ` +
      `headers=${String(this.headers)}, key=${show(this.key)}, ` +
      `value=${show(this.value)}, timestamp=${this.timestamp})`
    );
  }

  /**
   * The arguments of the native send: topic, partition and timestamp (-1 for
   * null), key and value bytes, which of them are the client's own bytes,
   * header pairs or null; then the record's headers are made read-only, as
   * Java's `doSend` seals them (`setReadOnly`) before handing the record
   * over. Called by `KafkaProducer`, whose `send()` rejects with what this
   * throws (the headers are then left writable).
   *
   * The fields are not frozen, so plain JavaScript may have reassigned one
   * since the constructor checked it: the constructor's checks run again on
   * the values read here, with its errors, as Java's `doSend` only ever reads
   * what its constructor accepted (each header's too). A valid record with
   * no headers allocates nothing more for them.
   *
   * @internal
   */
  internalToNative(): NativeRecord {
    const { topic, partition, timestamp, key, value, headers } = this;
    // The valid record's path: plain type and range tests, no call. Any other
    // value goes through the constructor's checks, which throw its error (or
    // accept it: an iterable of headers, read as the constructor reads one).
    if (
      typeof topic === 'string' &&
      (partition === null || (typeof partition === 'number' && Number.isInteger(partition) && partition >= 0 && partition <= INT32_MAX)) &&
      (timestamp === null || (typeof timestamp === 'number' && Number.isInteger(timestamp) && timestamp >= 0 && timestamp <= Number.MAX_SAFE_INTEGER)) &&
      (key === null || typeof key === 'string' || isBytes(key)) &&
      (value === null || typeof value === 'string' || isBytes(value)) &&
      headers instanceof Headers
    ) {
      const nativeHeaders = headers.internalToNative();
      const out: NativeRecord = [
        topic,
        partition === null ? -1 : partition,
        timestamp === null ? -1 : timestamp,
        toBytes(key),
        toBytes(value),
        ownedBytes(key, value),
        nativeHeaders,
      ];
      headers.internalSetReadOnly();
      return out;
    }
    return this.#checkedToNative();
  }

  // internalToNative for a record with a field the fast path did not accept:
  // the constructor's checks, in its order, then the same arguments.
  #checkedToNative(): NativeRecord {
    const topic = checkTopic(defined(this.topic, 'topic'));
    const timestamp = checkTimestamp(defined(this.timestamp, 'timestamp'));
    const partition = checkPartition(defined(this.partition, 'partition'));
    const key = checkData(defined(this.key, 'key'), 'key');
    const value = checkData(defined(this.value, 'value'), 'value');
    const given: unknown = defined(this.headers, 'headers');
    const headers = given instanceof Headers ? given : checkHeaders(given);
    const nativeHeaders = headers.internalToNative();
    const out: NativeRecord = [
      topic,
      partition === null ? -1 : partition,
      timestamp === null ? -1 : timestamp,
      toBytes(key),
      toBytes(value),
      ownedBytes(key, value),
      nativeHeaders,
    ];
    headers.internalSetReadOnly();
    return out;
  }
}

function show(value: Data | null): string {
  if (value === null) return 'null';
  if (isBytes(value)) return bytesToString(value);
  return String(value);
}
