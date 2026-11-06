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
// Uint8Array, a string (encoded as UTF-8 at send, Java's StringSerializer) or
// null.

import { illegalArgument } from '../_errors';

/**
 * A key, value or header value: bytes, or a string encoded as UTF-8.
 *
 * @jsOnly The JavaScript types a key, value or header value takes; Java's are
 *   the serializers' generic types.
 */
export type Data = Buffer | Uint8Array | string;

/**
 * A record header, Java's Header.
 *
 * @java org.apache.kafka.common.header.Header
 */
export type Header = {
  /** The header's key */
  key: string;
  /** The header's value */
  value: Data | null;
};

/**
 * Options of {@link ProducerRecord}'s constructor: Java's overloaded
 * constructors in one.
 *
 * @jsOnly The options object of one Java overload set, the constructors
 *   (javascript/CLAUDE.md "Java shape, JS idiom").
 */
export type ProducerRecordOptions = {
  /** The topic the record will be appended to */
  topic: string;
  /** The partition to which the record should be sent */
  partition?: number | null | undefined;
  /**
   * The timestamp of the record, in milliseconds since epoch; null: the
   * producer assigns its current time
   */
  timestamp?: number | null | undefined;
  /** The key that will be included in the record */
  key?: Data | null | undefined;
  /** The record contents */
  value?: Data | null | undefined;
  /** The headers that will be included in the record */
  headers?: Iterable<Header> | null | undefined;
};

/** A header as the native send takes it: key and value bytes. */
type NativeHeader = [string, Uint8Array | null];

/** The record arguments of the native send (`_native.ts` `producerSend`). */
type NativeRecord = [string, number, number, Uint8Array | null, Uint8Array | null, NativeHeader[] | null];

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
  return typeof value === 'string' ? Buffer.from(value, 'utf8') : value;
}

function checkHeaders(headers: unknown): ReadonlyArray<Readonly<Header>> {
  if (headers === undefined || headers === null) return Object.freeze([]);
  if (
    typeof headers !== 'object' ||
    typeof (headers as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function' ||
    typeof headers === 'string'
  ) {
    throw new TypeError('headers must be an iterable of {key, value}');
  }
  const out: Array<Readonly<Header>> = [];
  for (const header of headers as Iterable<unknown>) {
    if (header === null || typeof header !== 'object') throw new TypeError('a header must be a {key, value} object');
    const { key, value } = header as { key?: unknown; value?: unknown };
    // Java's RecordHeader: Objects.requireNonNull(key, "Null header keys are not permitted").
    if (key === undefined || key === null) throw new TypeError('Null header keys are not permitted');
    if (typeof key !== 'string') throw new TypeError('a header key must be a string');
    out.push(Object.freeze({ key, value: checkData(value, 'a header value') }));
  }
  return Object.freeze(out);
}

// Java's Integer / Long argument: null, or an integer number.
function checkOptionalInteger(value: unknown, what: string, max: number): number | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError(`${what} must be an integer or null`);
  if (value > max) throw new RangeError(`${what} must be at most ${max}`);
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
  #topic: string;
  #partition: number | null;
  #timestamp: number | null;
  #key: Data | null;
  #value: Data | null;
  #headers: ReadonlyArray<Readonly<Header>>;

  /**
   * Creates a record with a specified timestamp to be sent to a specified
   * topic and partition. Java's overloaded constructors are this one options
   * object; a field left out is what Java's shorter overload passes (`null`,
   * no headers).
   *
   * @param options - The topic, and optionally the partition, the timestamp
   *   (in milliseconds since epoch; if null, the producer will assign the
   *   timestamp using System.currentTimeMillis()), the key, the value and the
   *   headers of the record
   * @throws `IllegalArgumentError` when the topic is null, or the timestamp or
   *   the partition is negative, as Java's IllegalArgumentException
   * @throws `TypeError` when an argument has the wrong type
   * @throws `RangeError` when the partition is above 2^31 - 1 or the timestamp
   *   above 2^53 - 1, beyond Java's `Integer` and the integers a JS number
   *   holds exactly
   */
  constructor(options: ProducerRecordOptions) {
    if (options === null || typeof options !== 'object') {
      throw new TypeError('ProducerRecord takes an options object {topic, partition, timestamp, key, value, headers}');
    }
    const { topic, partition, timestamp, key, value, headers } = options;
    if (topic === undefined || topic === null) throw illegalArgument('Topic cannot be null.');
    if (typeof topic !== 'string') throw new TypeError('topic must be a string');
    this.#timestamp = checkOptionalInteger(timestamp, 'timestamp', Number.MAX_SAFE_INTEGER);
    if (this.#timestamp !== null && this.#timestamp < 0) {
      throw illegalArgument(
        `Invalid timestamp: ${this.#timestamp}. Timestamp should always be non-negative or null.`,
      );
    }
    this.#partition = checkOptionalInteger(partition, 'partition', INT32_MAX);
    if (this.#partition !== null && this.#partition < 0) {
      throw illegalArgument(
        `Invalid partition: ${this.#partition}. Partition number should always be non-negative or null.`,
      );
    }
    this.#topic = topic;
    this.#key = checkData(key, 'key');
    this.#value = checkData(value, 'value');
    this.#headers = checkHeaders(headers);
  }

  /** The topic this record is being sent to */
  get topic(): string {
    return this.#topic;
  }

  /** The headers */
  get headers(): ReadonlyArray<Readonly<Header>> {
    return this.#headers;
  }

  /** The key (or null if no key is specified) */
  get key(): Data | null {
    return this.#key;
  }

  /** The value */
  get value(): Data | null {
    return this.#value;
  }

  /** The timestamp, which is in milliseconds since epoch. */
  get timestamp(): number | null {
    return this.#timestamp;
  }

  /** The partition to which the record will be sent (or null if no partition was specified) */
  get partition(): number | null {
    return this.#partition;
  }

  toString(): string {
    // A header value is bytes in Java (RecordHeader.toString prints
    // Arrays.toString(value())): a string one prints as its UTF-8 bytes.
    const headers = this.#headers
      .map((h) => `RecordHeader(key = ${h.key}, value = ${show(typeof h.value === 'string' ? toBytes(h.value) : h.value)})`)
      .join(', ');
    return (
      `ProducerRecord(topic=${this.#topic}, partition=${this.#partition}, ` +
      `headers=RecordHeaders(headers = [${headers}], isReadOnly = false), key=${show(this.#key)}, ` +
      `value=${show(this.#value)}, timestamp=${this.#timestamp})`
    );
  }

  /**
   * The arguments of the native send (private): topic, partition and
   * timestamp (-1 for null), key and value bytes, header pairs or null.
   * Called by `KafkaProducer` (through `record['_toNative']`, which
   * TypeScript's `private` allows).
   */
  private _toNative(): NativeRecord {
    return [
      this.#topic,
      this.#partition === null ? -1 : this.#partition,
      this.#timestamp === null ? -1 : this.#timestamp,
      toBytes(this.#key),
      toBytes(this.#value),
      this.#headers.length === 0 ? null : this.#headers.map((h) => [h.key, toBytes(h.value)]),
    ];
  }
}

function show(value: Data | null): string {
  if (value === null) return 'null';
  if (isBytes(value)) return `[${Array.from(value, (b) => (b << 24) >> 24).join(', ')}]`;
  return String(value);
}
