// Copyright 2026 Confluent Inc.
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

// Java's ConsumerRecord<K, V>, a data class (javascript/CLAUDE.md, JavaScript
// Binding Conventions, Class family): its getters are readonly fields, and the
// object is frozen when it is created. Java's three public constructors are one
// constructor, told apart by the keys given (Signatures). `offset` and
// `timestamp` are Java `long`s, so bigints; `leaderEpoch` and `deliveryCount`
// are Java's `Optional` getters, `number | null` (Types). The headers go through
// the `headers` converter of src/internal/payload.ts: a `Headers` is kept, and
// an iterable of `Header` objects and `[key, value]` pairs is held by a new one.

import type { Header } from '../common/header/header';
import { Headers } from '../common/header/headers';
import { TimestampType } from '../common/record/timestamp_type';
import { JavaForms } from '../internal/args';
import { illegalArgument } from '../internal/errors';
import { checkInt, checkLong } from '../internal/java_number';
import { show } from '../internal/java_string';
import { headers as headersKind } from '../internal/payload';

// Java's constructors, in Java declaration order (src/internal/args.ts); a
// form error names the parameters in the longest form's order.
const FORMS = new JavaForms(
  'ConsumerRecord',
  [
    ['topic', 'partition', 'offset', 'key', 'value'],
    [
      'topic',
      'partition',
      'offset',
      'timestamp',
      'timestampType',
      'serializedKeySize',
      'serializedValueSize',
      'key',
      'value',
      'headers',
      'leaderEpoch',
    ],
    [
      'topic',
      'partition',
      'offset',
      'timestamp',
      'timestampType',
      'serializedKeySize',
      'serializedValueSize',
      'key',
      'value',
      'headers',
      'leaderEpoch',
      'deliveryCount',
    ],
  ],
  {
    order: [
      'topic',
      'partition',
      'offset',
      'timestamp',
      'timestampType',
      'serializedKeySize',
      'serializedValueSize',
      'key',
      'value',
      'headers',
      'leaderEpoch',
      'deliveryCount',
    ],
  },
);

// Java's Optional<Integer> argument: null, or a whole number in Java's int range.
function optionalInt(value: unknown, what: string): number | null {
  return value === null ? null : checkInt(value, what);
}

// Java's Optional<Short> argument: null, or a whole number in Java's short range.
function optionalShort(value: unknown, what: string): number | null {
  if (value === null) return null;
  const n = checkInt(value, what);
  if (n < -32768 || n > 32767) throw new RangeError(`${what} must be from -32768 to 32767`);
  return n;
}

/**
 * A key/value pair to be received from Kafka. This also consists of a topic
 * name and a partition number from which the record is being received, an
 * offset that points to the record in a Kafka partition, and a timestamp as
 * marked by the corresponding ProducerRecord.
 */
export class ConsumerRecord<out K = never, out V = never> {
  /** Java's `NO_TIMESTAMP`, the timestamp of a record that has none. */
  static readonly NO_TIMESTAMP: bigint = -1n;

  /** Java's `NULL_SIZE`, the serialized size of a null key or value. */
  static readonly NULL_SIZE: number = -1;

  /** The topic this record is received from (never null) */
  readonly topic: string;

  /** The partition from which this record is received */
  readonly partition: number;

  /** The position of this record in the corresponding Kafka partition. */
  readonly offset: bigint;

  /** The timestamp of this record, in milliseconds elapsed since unix epoch. */
  readonly timestamp: bigint;

  /** The timestamp type of this record */
  readonly timestampType: TimestampType;

  /** The size of the serialized, uncompressed key in bytes. If key is null, the returned size is -1. */
  readonly serializedKeySize: number;

  /** The size of the serialized, uncompressed value in bytes. If value is null, the returned size is -1. */
  readonly serializedValueSize: number;

  /** The headers (never null) */
  readonly headers: Headers;

  /** The key (or null if no key is specified) */
  readonly key: K | null;

  /** The value */
  readonly value: V | null;

  /** The leader epoch for the record if available, or null for legacy record formats. */
  readonly leaderEpoch: number | null;

  /**
   * The delivery count for the record if available, or null when deliveries
   * are not counted. Deliveries are counted for records delivered by share
   * groups.
   */
  readonly deliveryCount: number | null;

  /**
   * Creates a record of Java's five-parameter form with a null key and a null value,
   * which binds `never`.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic this record is received from. */
    topic: string;
    /** The partition of the topic this record is received from. */
    partition: number;
    /** The offset of this record in the corresponding Kafka partition: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    timestamp?: never;
    timestampType?: never;
    serializedKeySize?: never;
    serializedValueSize?: never;
    /** A null key. */
    key: null;
    /** A null value. */
    value: null;
    headers?: never;
    leaderEpoch?: never;
    deliveryCount?: never;
  });
  /**
   * Creates a record of Java's five-parameter form with a null key,
   * which binds `never`.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic this record is received from. */
    topic: string;
    /** The partition of the topic this record is received from. */
    partition: number;
    /** The offset of this record in the corresponding Kafka partition: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    timestamp?: never;
    timestampType?: never;
    serializedKeySize?: never;
    serializedValueSize?: never;
    /** A null key. */
    key: null;
    /** The record contents, or null. */
    value: V | null;
    headers?: never;
    leaderEpoch?: never;
    deliveryCount?: never;
  });
  /**
   * Creates a record of Java's five-parameter form with a null value,
   * which binds `never`.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic this record is received from. */
    topic: string;
    /** The partition of the topic this record is received from. */
    partition: number;
    /** The offset of this record in the corresponding Kafka partition: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    timestamp?: never;
    timestampType?: never;
    serializedKeySize?: never;
    serializedValueSize?: never;
    /** The key of the record, if one exists (null is allowed). */
    key: K | null;
    /** A null value. */
    value: null;
    headers?: never;
    leaderEpoch?: never;
    deliveryCount?: never;
  });
  /**
   * Creates a record to be received from a specified topic and partition (provided for
   * compatibility with Kafka 0.9 before the message format supported timestamps and before
   * serialized metadata were exposed).
   *
   * @param options - the record's fields
   * @throws `IllegalArgumentError` when the given parameters are not one of
   *   Java's constructors, or the topic is null
   * @throws `TypeError` when an argument has the wrong type or is `undefined`, or
   *   a header is not a `Header` or a `[key, value]` pair
   * @throws `RangeError` when a number is beyond its Java type, or a `long`
   *   given as a number is beyond 2^53 - 1
   */
  constructor(options: {
    /** The topic this record is received from. */
    topic: string;
    /** The partition of the topic this record is received from. */
    partition: number;
    /** The offset of this record in the corresponding Kafka partition: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    timestamp?: never;
    timestampType?: never;
    serializedKeySize?: never;
    serializedValueSize?: never;
    /** The key of the record, if one exists (null is allowed). */
    key: K | null;
    /** The record contents, or null. */
    value: V | null;
    headers?: never;
    leaderEpoch?: never;
    deliveryCount?: never;
  });
  /**
   * Creates a record of Java's eleven-parameter form with a null key and a null value,
   * which binds `never`.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic this record is received from. */
    topic: string;
    /** The partition of the topic this record is received from. */
    partition: number;
    /** The offset of this record in the corresponding Kafka partition: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    /** The timestamp of the record: a bigint, or a whole number up to 2^53 - 1. */
    timestamp: bigint | number;
    /** The timestamp type. */
    timestampType: TimestampType;
    /** The length of the serialized key. */
    serializedKeySize: number;
    /** The length of the serialized value. */
    serializedValueSize: number;
    /** A null key. */
    key: null;
    /** A null value. */
    value: null;
    /** The headers of the record: a `Headers`, kept as it is, or an iterable of `Header` objects and `[key, value]` pairs, held by a new `Headers`. */
    headers: Iterable<Header | readonly [string, Uint8Array | null]>;
    /** Optional leader epoch of the record (null for legacy record formats). */
    leaderEpoch: number | null;
    deliveryCount?: never;
  });
  /**
   * Creates a record of Java's eleven-parameter form with a null key,
   * which binds `never`.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic this record is received from. */
    topic: string;
    /** The partition of the topic this record is received from. */
    partition: number;
    /** The offset of this record in the corresponding Kafka partition: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    /** The timestamp of the record: a bigint, or a whole number up to 2^53 - 1. */
    timestamp: bigint | number;
    /** The timestamp type. */
    timestampType: TimestampType;
    /** The length of the serialized key. */
    serializedKeySize: number;
    /** The length of the serialized value. */
    serializedValueSize: number;
    /** A null key. */
    key: null;
    /** The record contents, or null. */
    value: V | null;
    /** The headers of the record: a `Headers`, kept as it is, or an iterable of `Header` objects and `[key, value]` pairs, held by a new `Headers`. */
    headers: Iterable<Header | readonly [string, Uint8Array | null]>;
    /** Optional leader epoch of the record (null for legacy record formats). */
    leaderEpoch: number | null;
    deliveryCount?: never;
  });
  /**
   * Creates a record of Java's eleven-parameter form with a null value,
   * which binds `never`.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic this record is received from. */
    topic: string;
    /** The partition of the topic this record is received from. */
    partition: number;
    /** The offset of this record in the corresponding Kafka partition: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    /** The timestamp of the record: a bigint, or a whole number up to 2^53 - 1. */
    timestamp: bigint | number;
    /** The timestamp type. */
    timestampType: TimestampType;
    /** The length of the serialized key. */
    serializedKeySize: number;
    /** The length of the serialized value. */
    serializedValueSize: number;
    /** The key of the record, if one exists (null is allowed). */
    key: K | null;
    /** A null value. */
    value: null;
    /** The headers of the record: a `Headers`, kept as it is, or an iterable of `Header` objects and `[key, value]` pairs, held by a new `Headers`. */
    headers: Iterable<Header | readonly [string, Uint8Array | null]>;
    /** Optional leader epoch of the record (null for legacy record formats). */
    leaderEpoch: number | null;
    deliveryCount?: never;
  });
  /**
   * Creates a record to be received from a specified topic and partition.
   *
   * @param options - the record's fields
   * @throws `IllegalArgumentError` when the topic or the headers are null
   */
  constructor(options: {
    /** The topic this record is received from. */
    topic: string;
    /** The partition of the topic this record is received from. */
    partition: number;
    /** The offset of this record in the corresponding Kafka partition: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    /** The timestamp of the record: a bigint, or a whole number up to 2^53 - 1. */
    timestamp: bigint | number;
    /** The timestamp type. */
    timestampType: TimestampType;
    /** The length of the serialized key. */
    serializedKeySize: number;
    /** The length of the serialized value. */
    serializedValueSize: number;
    /** The key of the record, if one exists (null is allowed). */
    key: K | null;
    /** The record contents, or null. */
    value: V | null;
    /** The headers of the record: a `Headers`, kept as it is, or an iterable of `Header` objects and `[key, value]` pairs, held by a new `Headers`. */
    headers: Iterable<Header | readonly [string, Uint8Array | null]>;
    /** Optional leader epoch of the record (null for legacy record formats). */
    leaderEpoch: number | null;
    deliveryCount?: never;
  });
  /**
   * Creates a record of Java's twelve-parameter form with a null key and a null value,
   * which binds `never`.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic this record is received from. */
    topic: string;
    /** The partition of the topic this record is received from. */
    partition: number;
    /** The offset of this record in the corresponding Kafka partition: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    /** The timestamp of the record: a bigint, or a whole number up to 2^53 - 1. */
    timestamp: bigint | number;
    /** The timestamp type. */
    timestampType: TimestampType;
    /** The length of the serialized key. */
    serializedKeySize: number;
    /** The length of the serialized value. */
    serializedValueSize: number;
    /** A null key. */
    key: null;
    /** A null value. */
    value: null;
    /** The headers of the record: a `Headers`, kept as it is, or an iterable of `Header` objects and `[key, value]` pairs, held by a new `Headers`. */
    headers: Iterable<Header | readonly [string, Uint8Array | null]>;
    /** Optional leader epoch of the record (null for legacy record formats). */
    leaderEpoch: number | null;
    /** Optional delivery count of the record (null when deliveries are not counted). */
    deliveryCount: number | null;
  });
  /**
   * Creates a record of Java's twelve-parameter form with a null key,
   * which binds `never`.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic this record is received from. */
    topic: string;
    /** The partition of the topic this record is received from. */
    partition: number;
    /** The offset of this record in the corresponding Kafka partition: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    /** The timestamp of the record: a bigint, or a whole number up to 2^53 - 1. */
    timestamp: bigint | number;
    /** The timestamp type. */
    timestampType: TimestampType;
    /** The length of the serialized key. */
    serializedKeySize: number;
    /** The length of the serialized value. */
    serializedValueSize: number;
    /** A null key. */
    key: null;
    /** The record contents, or null. */
    value: V | null;
    /** The headers of the record: a `Headers`, kept as it is, or an iterable of `Header` objects and `[key, value]` pairs, held by a new `Headers`. */
    headers: Iterable<Header | readonly [string, Uint8Array | null]>;
    /** Optional leader epoch of the record (null for legacy record formats). */
    leaderEpoch: number | null;
    /** Optional delivery count of the record (null when deliveries are not counted). */
    deliveryCount: number | null;
  });
  /**
   * Creates a record of Java's twelve-parameter form with a null value,
   * which binds `never`.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic this record is received from. */
    topic: string;
    /** The partition of the topic this record is received from. */
    partition: number;
    /** The offset of this record in the corresponding Kafka partition: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    /** The timestamp of the record: a bigint, or a whole number up to 2^53 - 1. */
    timestamp: bigint | number;
    /** The timestamp type. */
    timestampType: TimestampType;
    /** The length of the serialized key. */
    serializedKeySize: number;
    /** The length of the serialized value. */
    serializedValueSize: number;
    /** The key of the record, if one exists (null is allowed). */
    key: K | null;
    /** A null value. */
    value: null;
    /** The headers of the record: a `Headers`, kept as it is, or an iterable of `Header` objects and `[key, value]` pairs, held by a new `Headers`. */
    headers: Iterable<Header | readonly [string, Uint8Array | null]>;
    /** Optional leader epoch of the record (null for legacy record formats). */
    leaderEpoch: number | null;
    /** Optional delivery count of the record (null when deliveries are not counted). */
    deliveryCount: number | null;
  });
  /**
   * Creates a record to be received from a specified topic and partition.
   *
   * @param options - the record's fields
   */
  constructor(options: {
    /** The topic this record is received from. */
    topic: string;
    /** The partition of the topic this record is received from. */
    partition: number;
    /** The offset of this record in the corresponding Kafka partition: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    /** The timestamp of the record: a bigint, or a whole number up to 2^53 - 1. */
    timestamp: bigint | number;
    /** The timestamp type. */
    timestampType: TimestampType;
    /** The length of the serialized key. */
    serializedKeySize: number;
    /** The length of the serialized value. */
    serializedValueSize: number;
    /** The key of the record, if one exists (null is allowed). */
    key: K | null;
    /** The record contents, or null. */
    value: V | null;
    /** The headers of the record: a `Headers`, kept as it is, or an iterable of `Header` objects and `[key, value]` pairs, held by a new `Headers`. */
    headers: Iterable<Header | readonly [string, Uint8Array | null]>;
    /** Optional leader epoch of the record (null for legacy record formats). */
    leaderEpoch: number | null;
    /** Optional delivery count of the record (null when deliveries are not counted). */
    deliveryCount: number | null;
  });
  constructor(options: unknown) {
    const form = FORMS.match(options, arguments.length);
    const topic = FORMS.value(options, 'topic');
    if (topic !== null && typeof topic !== 'string') throw new TypeError('topic must be a string');
    const partition = checkInt(FORMS.value(options, 'partition'), 'partition');
    const offset = checkLong(FORMS.value(options, 'offset'), 'offset');
    const key = FORMS.value(options, 'key') as K | null;
    const value = FORMS.value(options, 'value') as V | null;
    let timestamp = ConsumerRecord.NO_TIMESTAMP;
    let timestampType = TimestampType.NO_TIMESTAMP_TYPE;
    let serializedKeySize = ConsumerRecord.NULL_SIZE;
    let serializedValueSize = ConsumerRecord.NULL_SIZE;
    let headers: unknown = null;
    let leaderEpoch: number | null = null;
    let deliveryCount: number | null = null;
    if (form === 0) {
      // Java's short constructor passes these.
      headers = new Headers();
    } else {
      timestamp = checkLong(FORMS.value(options, 'timestamp'), 'timestamp');
      const type = FORMS.value(options, 'timestampType');
      // Java only stores the timestamp type, a null one included.
      if (type !== null && !(type instanceof TimestampType)) throw new TypeError('timestampType must be a TimestampType');
      timestampType = type as TimestampType;
      serializedKeySize = checkInt(FORMS.value(options, 'serializedKeySize'), 'serializedKeySize');
      serializedValueSize = checkInt(FORMS.value(options, 'serializedValueSize'), 'serializedValueSize');
      leaderEpoch = optionalInt(FORMS.value(options, 'leaderEpoch'), 'leaderEpoch');
      if (form === 2) deliveryCount = optionalShort(FORMS.value(options, 'deliveryCount'), 'deliveryCount');
      // The caller's `new RecordHeaders(...)` and `new RecordHeader(...)`, whose
      // errors Java throws before the constructor runs.
      headers = headersKind.take(FORMS.value(options, 'headers'), 'headers');
    }
    if (topic === null) throw illegalArgument('Topic cannot be null');
    if (headers === null) throw illegalArgument('Headers cannot be null');
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.timestampType = timestampType;
    this.serializedKeySize = serializedKeySize;
    this.serializedValueSize = serializedValueSize;
    this.headers = headers as Headers;
    this.key = key;
    this.value = value;
    this.leaderEpoch = leaderEpoch;
    this.deliveryCount = deliveryCount;
    Object.freeze(this);
  }

  /**
   * A record the consumer fetched, from the fields the addon read: built
   * without the constructor's checks, which the core's values need not pass
   * again, in the constructor's field order, and frozen. Called by the addon
   * for each record of a poll.
   *
   * @param topic - the partition's topic, one string per partition
   * @param partition - the partition
   * @param offset - the offset
   * @param timestamp - the timestamp
   * @param timestampTypeId - the timestamp type's Java `id`
   * @param serializedKeySize - the key's size, -1 for a null key
   * @param serializedValueSize - the value's size, -1 for a null value
   * @param headers - the addon's handle of the record's headers, or null for none
   * @param key - the key's bytes, or null
   * @param value - the value's bytes, or null
   * @param leaderEpoch - the leader epoch, -1 for none
   * @param deliveryCount - the delivery count, -1 for none
   * @returns the record
   *
   * @internal
   */
  static internalCreate(
    topic: string,
    partition: number,
    offset: bigint,
    timestamp: bigint,
    timestampTypeId: number,
    serializedKeySize: number,
    serializedValueSize: number,
    headers: object | null,
    key: Buffer | null,
    value: Buffer | null,
    leaderEpoch: number,
    deliveryCount: number,
  ): ConsumerRecord<Buffer, Buffer> {
    const record = Object.create(ConsumerRecord.prototype) as Record<string, unknown>;
    record.topic = topic;
    record.partition = partition;
    record.offset = offset;
    record.timestamp = timestamp;
    record.timestampType =
      timestampTypeId === 0
        ? TimestampType.CREATE_TIME
        : timestampTypeId === 1
          ? TimestampType.LOG_APPEND_TIME
          : TimestampType.NO_TIMESTAMP_TYPE;
    record.serializedKeySize = serializedKeySize;
    record.serializedValueSize = serializedValueSize;
    record.headers = headers === null ? new Headers() : Headers.internalOfConsumedRecord(headers);
    record.key = key;
    record.value = value;
    record.leaderEpoch = leaderEpoch < 0 ? null : leaderEpoch;
    record.deliveryCount = deliveryCount < 0 ? null : deliveryCount;
    return Object.freeze(record) as unknown as ConsumerRecord<Buffer, Buffer>;
  }

  /**
   * The record as Java's `ConsumerRecord.toString()` writes it.
   *
   * @returns `ConsumerRecord(topic = ..., partition = ..., leaderEpoch = ...,
   *   offset = ..., <timestamp type> = ..., deliveryCount = ..., serialized key
   *   size = ..., serialized value size = ..., headers = ..., key = ..., value = ...)`
   */
  toString(): string {
    return (
      `ConsumerRecord(topic = ${this.topic}, partition = ${this.partition}, leaderEpoch = ${String(this.leaderEpoch)}, ` +
      `offset = ${this.offset}, ${String(this.timestampType)} = ${this.timestamp}, ` +
      `deliveryCount = ${String(this.deliveryCount)}, serialized key size = ${this.serializedKeySize}, ` +
      `serialized value size = ${this.serializedValueSize}, headers = ${String(this.headers)}, ` +
      `key = ${show(this.key)}, value = ${show(this.value)})`
    );
  }

  /**
   * The record as `JSON.stringify` writes it: its fields in their order, the
   * offset and the timestamp decimal strings, which `BigInt()` reads back
   * exactly. It ignores the key `JSON.stringify` passes.
   *
   * @returns the record's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol: `JSON.stringify` throws
   * on a `bigint`, and a `number` loses digits above 2^53.
   */
  toJSON(): {
    topic: string;
    partition: number;
    offset: string;
    timestamp: string;
    timestampType: TimestampType;
    serializedKeySize: number;
    serializedValueSize: number;
    headers: Headers;
    key: K | null;
    value: V | null;
    leaderEpoch: number | null;
    deliveryCount: number | null;
  } {
    return {
      topic: this.topic,
      partition: this.partition,
      offset: this.offset.toString(),
      timestamp: this.timestamp.toString(),
      timestampType: this.timestampType,
      serializedKeySize: this.serializedKeySize,
      serializedValueSize: this.serializedValueSize,
      headers: this.headers,
      key: this.key,
      value: this.value,
      leaderEpoch: this.leaderEpoch,
      deliveryCount: this.deliveryCount,
    };
  }
}
