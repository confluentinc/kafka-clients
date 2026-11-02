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

// Java's ProducerRecord<K, V> over bytes: a key and a value are a Buffer, a
// Uint8Array, a string (encoded as UTF-8 at send, Java's StringSerializer) or
// null.

const { illegalArgument } = require('../common/kafka_error');

const INT32_MAX = 2 ** 31 - 1;

function isBytes(value) {
  return value instanceof Uint8Array;
}

function checkData(value, what) {
  if (value === undefined || value === null) return null;
  if (typeof value === 'string' || isBytes(value)) return value;
  throw new TypeError(`${what} must be a Buffer, a Uint8Array, a string or null`);
}

function toBytes(value) {
  if (value === null) return null;
  return typeof value === 'string' ? Buffer.from(value, 'utf8') : value;
}

function checkHeaders(headers) {
  if (headers === undefined || headers === null) return Object.freeze([]);
  if (typeof headers !== 'object' || typeof headers[Symbol.iterator] !== 'function' || typeof headers === 'string') {
    throw new TypeError('headers must be an iterable of {key, value}');
  }
  const out = [];
  for (const header of headers) {
    if (header === null || typeof header !== 'object') throw new TypeError('a header must be a {key, value} object');
    const { key, value } = header;
    // Java's RecordHeader: Objects.requireNonNull(key, "Null header keys are not permitted").
    if (key === undefined || key === null) throw new TypeError('Null header keys are not permitted');
    if (typeof key !== 'string') throw new TypeError('a header key must be a string');
    out.push(Object.freeze({ key, value: checkData(value, 'a header value') }));
  }
  return Object.freeze(out);
}

// Java's Integer / Long argument: null, or an integer number.
function checkOptionalInteger(value, what, max) {
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
class ProducerRecord {
  #topic;
  #partition;
  #timestamp;
  #key;
  #value;
  #headers;

  /**
   * Creates a record with a specified timestamp to be sent to a specified
   * topic and partition. Java's overloaded constructors are this one options
   * object; a field left out is what Java's shorter overload passes (`null`,
   * no headers).
   *
   * @param {object} options
   * @param {string} options.topic The topic the record will be appended to
   * @param {number | null} [options.partition] The partition to which the record should be sent
   * @param {number | null} [options.timestamp] The timestamp of the record, in milliseconds since
   *   epoch. If null, the producer will assign the timestamp using System.currentTimeMillis().
   * @param {Buffer | Uint8Array | string | null} [options.key] The key that will be included in the record
   * @param {Buffer | Uint8Array | string | null} [options.value] The record contents
   * @param {Iterable<{key: string, value: Buffer | Uint8Array | string | null}> | null} [options.headers]
   *   the headers that will be included in the record
   * @throws {KafkaError} `LOCAL_ILLEGAL_ARGUMENT` when the topic is null, or the timestamp or the
   *   partition is negative, as Java's IllegalArgumentException
   * @throws {TypeError} when an argument has the wrong type
   */
  constructor(options) {
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

  /** @returns {string} The topic this record is being sent to */
  get topic() {
    return this.#topic;
  }

  /** @returns {ReadonlyArray<{key: string, value: Buffer | Uint8Array | string | null}>} The headers */
  get headers() {
    return this.#headers;
  }

  /** @returns {Buffer | Uint8Array | string | null} The key (or null if no key is specified) */
  get key() {
    return this.#key;
  }

  /** @returns {Buffer | Uint8Array | string | null} The value */
  get value() {
    return this.#value;
  }

  /** @returns {number | null} The timestamp, which is in milliseconds since epoch. */
  get timestamp() {
    return this.#timestamp;
  }

  /** @returns {number | null} The partition to which the record will be sent (or null if no partition was specified) */
  get partition() {
    return this.#partition;
  }

  toString() {
    const headers = this.#headers.map((h) => `RecordHeader(key = ${h.key}, value = ${show(h.value)})`).join(', ');
    return (
      `ProducerRecord(topic=${this.#topic}, partition=${this.#partition}, ` +
      `headers=RecordHeaders(headers = [${headers}], isReadOnly = false), key=${show(this.#key)}, ` +
      `value=${show(this.#value)}, timestamp=${this.#timestamp})`
    );
  }

  /**
   * The arguments of the native send (private): topic, partition and
   * timestamp (-1 for null), key and value bytes, header pairs or null.
   */
  _toNative() {
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

function show(value) {
  if (value === null) return 'null';
  if (isBytes(value)) return `[${Array.from(value, (b) => (b << 24) >> 24).join(', ')}]`;
  return String(value);
}

module.exports = { ProducerRecord };
