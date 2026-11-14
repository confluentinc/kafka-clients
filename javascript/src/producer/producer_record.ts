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
import { objectHashCode, objectsEquals } from '../internal/java_object';
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
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/** A header as the native send takes it: key and value bytes. */
type NativeHeader = [Uint8Array, Uint8Array | null];

/**
 * The record arguments of the native send (`internal/native.ts`
 * `producerSend`): topic, partition, timestamp, key, value, headers.
 */
type NativeRecord = [string, number, bigint, Uint8Array | null, Uint8Array | null, NativeHeader[] | null];

const INT32_MIN = -(2 ** 31);
const INT32_MAX = 2 ** 31 - 1;
const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

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

// Java's `new RecordHeaders(headers)`: new headers holding the given ones.
function checkHeaders(headers: unknown): Headers {
  return Headers.internalTaken(headers === undefined ? null : headers);
}

// Java's Integer argument: null, or a whole number in Java's int range.
function checkOptionalInt(value: unknown, what: string): number | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError(`${what} must be an integer or null`);
  if (value < INT32_MIN || value > INT32_MAX) throw new RangeError(`${what} must be from ${INT32_MIN} to ${INT32_MAX}`);
  return value;
}

// Java's Long argument: null, a bigint in Java's long range, or a whole
// number a JavaScript number holds exactly, read as a bigint (root CLAUDE.md,
// JavaScript Binding Conventions, Types).
function checkOptionalLong(value: unknown, what: string): bigint | null {
  if (value === undefined || value === null) return null;
  if (typeof value === 'bigint') {
    if (value < INT64_MIN || value > INT64_MAX) throw new RangeError(`${what} must be from ${INT64_MIN} to ${INT64_MAX}`);
    return value;
  }
  if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError(`${what} must be a bigint, an integer or null`);
  if (Math.abs(value) > Number.MAX_SAFE_INTEGER) {
    throw new RangeError(`${what} must be a bigint beyond 2^53 - 1: a number holds no larger integer exactly`);
  }
  return BigInt(value);
}

// The constructor's checks of one argument: Java's, and the run-time types
// TypeScript cannot enforce.

function checkTopic(topic: unknown): string {
  if (topic === undefined || topic === null) throw illegalArgument('Topic cannot be null.');
  if (typeof topic !== 'string') throw new TypeError('topic must be a string');
  return topic;
}

function checkTimestamp(timestamp: unknown): bigint | null {
  const checked = checkOptionalLong(timestamp, 'timestamp');
  if (checked !== null && checked < 0n) {
    throw illegalArgument(`Invalid timestamp: ${checked}. Timestamp should always be non-negative or null.`);
  }
  return checked;
}

function checkPartition(partition: unknown): number | null {
  const checked = checkOptionalInt(partition, 'partition');
  if (checked !== null && checked < 0) {
    throw illegalArgument(`Invalid partition: ${checked}. Partition number should always be non-negative or null.`);
  }
  return checked;
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
  readonly timestamp: bigint | null;

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
   * @throws `RangeError` when the partition is beyond Java's `Integer`, the
   *   timestamp a bigint beyond Java's `Long`, or a number beyond 2^53 - 1,
   *   the integers a JS number holds exactly
   *
   * @remarks The parameters given are one of Java's constructors, each a
   * signature of its own: `{topic, partition, timestamp, key, value,
   * headers}`, `{topic, partition, timestamp, key, value}`, `{topic,
   * partition, key, value, headers}`, `{topic, partition, key, value}`,
   * `{topic, key, value}` or `{topic, value}`; one a constructor leaves out is
   * what it passes (`null`, no headers). `null` is Java's `null`; a key set to
   * `undefined` is refused (leave the key out). The record is frozen: a field
   * cannot be reassigned, so `send()` reads what this constructor checked.
   * Its `headers` stay writable until the record is sent.
   */
  constructor(options: {
    /** The topic the record will be appended to. */
    topic: string;
    /** The partition to which the record should be sent, or null. */
    partition: number | null;
    /**
     * The timestamp of the record, in milliseconds since epoch: a bigint, or
     * a whole number up to 2^53 - 1. If null, the producer will assign the
     * timestamp using System.currentTimeMillis().
     */
    timestamp: bigint | number | null;
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
     * The timestamp of the record, in milliseconds since epoch: a bigint, or
     * a whole number up to 2^53 - 1. If null, the producer will assign the
     * timestamp using System.currentTimeMillis().
     */
    timestamp: bigint | number | null;
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
    Object.freeze(this);
  }

  /**
   * Whether `o` is a `ProducerRecord` with equal fields, Java's
   * `ProducerRecord.equals`: `Objects.equals` on the key, the partition, the
   * topic, the headers, the value and the timestamp.
   *
   * @param options - the object to compare with
   * @returns true when `o` is an equal record
   * @throws `IllegalArgumentError` when the given parameters are not `{o}`
   *
   * @remarks A `Uint8Array` key or value is equal only to itself, as Java's
   * `byte[]` is; a string key or value is equal to an equal string.
   */
  equals(options: {
    /** The object to compare with. */
    o: unknown;
  }): boolean;
  /**
   * The same as `equals({o})`, with the object on its own; a plain object is
   * read as the options object.
   *
   * @param o - the object to compare with
   * @returns true when `o` is an equal record
   */
  equals(o: unknown): boolean;
  equals(options: unknown): boolean {
    EQUALS_FORMS.match(options, arguments.length);
    const o = EQUALS_FORMS.value(options, 'o');
    if (this === o) return true;
    else if (!(o instanceof ProducerRecord)) return false;
    const that = o;
    return (
      objectsEquals(this.key, that.key) &&
      objectsEquals(this.partition, that.partition) &&
      objectsEquals(this.topic, that.topic) &&
      objectsEquals(this.headers, that.headers) &&
      objectsEquals(this.value, that.value) &&
      objectsEquals(this.timestamp, that.timestamp)
    );
  }

  /**
   * The record's hash code, Java's `ProducerRecord.hashCode`: the topic's,
   * then `31 * result` plus the partition's, the headers', the key's, the
   * value's and the timestamp's, each 0 when null.
   *
   * @returns the hash code, a 32-bit integer
   * @throws `TypeError` when it is given an argument
   *
   * @remarks A `Uint8Array` key or value adds the same hash code, 0, whatever
   * its bytes: it is Java's `byte[]`, equal only to itself, whose identity hash
   * code Java leaves unspecified, so the record's hash code stays the same if
   * the Buffer is changed after `send()` settled.
   */
  hashCode(): number {
    HASH_CODE_FORMS.match(undefined, arguments.length);
    let result = objectHashCode(this.topic);
    result = (Math.imul(31, result) + objectHashCode(this.partition)) | 0;
    result = (Math.imul(31, result) + objectHashCode(this.headers)) | 0;
    result = (Math.imul(31, result) + objectHashCode(this.key)) | 0;
    result = (Math.imul(31, result) + objectHashCode(this.value)) | 0;
    result = (Math.imul(31, result) + objectHashCode(this.timestamp)) | 0;
    return result;
  }

  toString(): string {
    return (
      `ProducerRecord(topic=${this.topic}, partition=${this.partition}, ` +
      `headers=${String(this.headers)}, key=${show(this.key)}, ` +
      `value=${show(this.value)}, timestamp=${this.timestamp})`
    );
  }

  /**
   * The record as `JSON.stringify` writes it: its fields in their order, the
   * timestamp a decimal string (or null), which `BigInt()` reads back exactly.
   * It ignores the key `JSON.stringify` passes.
   *
   * @returns the record's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol: `JSON.stringify` throws
   * on a `bigint`, and a `number` loses digits above 2^53.
   */
  toJSON(): {
    topic: string;
    partition: number | null;
    timestamp: string | null;
    key: Data | null;
    value: Data | null;
    headers: Headers;
  } {
    return {
      topic: this.topic,
      partition: this.partition,
      timestamp: this.timestamp === null ? null : this.timestamp.toString(),
      key: this.key,
      value: this.value,
      headers: this.headers,
    };
  }

  /**
   * The arguments of the native send: topic, partition (-1 for null) and
   * timestamp (-1n for null), key and value bytes, header pairs or null; then
   * the record's headers are made read-only, as Java's `doSend` seals them
   * (`setReadOnly`) before handing the record over. Called by
   * `KafkaProducer`. The record is frozen, so its fields are what the
   * constructor checked: nothing is checked again. A record with no headers
   * allocates nothing more for them.
   *
   * @internal
   */
  internalToNative(): NativeRecord {
    const { topic, partition, timestamp, key, value, headers } = this;
    const out: NativeRecord = [
      topic,
      partition === null ? -1 : partition,
      timestamp === null ? -1n : timestamp,
      toBytes(key),
      toBytes(value),
      headers.internalToNative(),
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
