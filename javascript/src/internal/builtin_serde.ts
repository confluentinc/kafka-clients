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

// The checks Java's built-in serde classes get from Java's types and
// JavaScript's do not (private; javascript/CLAUDE.md, JavaScript Binding
// Conventions, Serialization and Types): a serializer given a value of another
// type throws SerializationError naming the JavaScript types, as Java's send()
// turns a serializer's ClassCastException into one, and a number beyond the
// Java type throws RangeError. KafkaProducer#send gives a type failure Java's
// message, which names the key or the value (src/internal/serde.ts,
// serializerFailure).

import { kind } from './args';
import { SerializationError } from '../common/errors/serialization_error';

// Marks a built-in serializer's type failure, Java's ClassCastException, which
// the producer answers with Java's "Can't convert ..." message.
const TYPE_FAILURE: unique symbol = Symbol('built-in serializer type failure');

/**
 * The failure of built-in serializer `serializer` given `data` of another
 * type than `takes`: Java's cast fails there.
 *
 * @param serializer - the serializer's class name
 * @param takes - the JavaScript types it takes
 * @param data - the value given
 * @returns the error to throw
 */
export function wrongType(serializer: string, takes: string, data: unknown): SerializationError {
  const error = new SerializationError({ message: `${serializer} takes ${takes}, not ${kind(data)}` });
  Object.defineProperty(error, TYPE_FAILURE, { value: true });
  return error;
}

/**
 * Whether `error` is a built-in serializer's type failure (`wrongType`).
 *
 * @param error - what a serializer threw
 * @returns whether it is a type failure
 */
export function isTypeFailure(error: unknown): boolean {
  return error instanceof SerializationError && Object.hasOwn(error, TYPE_FAILURE);
}

/**
 * A Java integer of `bits` bits given to `serializer`: a whole number within
 * the type, Java's `Short` or `Integer`.
 *
 * @param serializer - the serializer's class name
 * @param data - the value given
 * @param bits - 16 or 32
 * @returns the number
 *
 * @throws `SerializationError` when it is no number
 * @throws `RangeError` when it is not a whole number within the type
 */
export function javaInteger(serializer: string, data: unknown, bits: 16 | 32): number {
  if (typeof data !== 'number') throw wrongType(serializer, 'a number or null', data);
  const max = bits === 16 ? 0x7fff : 0x7fffffff;
  if (!Number.isInteger(data) || data < -max - 1 || data > max) {
    throw new RangeError(`${serializer} takes a whole number from ${-max - 1} to ${max}, not ${String(data)}`);
  }
  return data;
}

/**
 * A Java `long` given to `serializer`: a bigint within the type, or a whole
 * number up to 2^53 - 1.
 *
 * @param serializer - the serializer's class name
 * @param data - the value given
 * @returns the value as a bigint
 *
 * @throws `SerializationError` when it is neither a bigint nor a number
 * @throws `RangeError` when it is beyond the type, or a number that is not
 *   whole or beyond 2^53 - 1
 */
export function javaLong(serializer: string, data: unknown): bigint {
  if (typeof data === 'bigint') {
    if (data < -(1n << 63n) || data >= 1n << 63n) {
      throw new RangeError(`${serializer} takes a bigint from ${-(1n << 63n)} to ${(1n << 63n) - 1n}, not ${data}`);
    }
    return data;
  }
  if (typeof data !== 'number') throw wrongType(serializer, 'a bigint, a number or null', data);
  if (!Number.isSafeInteger(data)) {
    throw new RangeError(`${serializer} takes a whole number up to 2^53 - 1, or a bigint, not ${String(data)}`);
  }
  return BigInt(data);
}

/**
 * The bytes a built-in deserializer is given, which must be a `Uint8Array` (a
 * `Buffer` is one), as a `Buffer` over the same memory.
 *
 * @param deserializer - the deserializer's class name
 * @param data - the bytes given
 * @returns the bytes as a `Buffer`, not copied
 *
 * @throws `TypeError` when they are no `Uint8Array`
 */
export function deserializerBytes(deserializer: string, data: unknown): Buffer {
  if (Buffer.isBuffer(data)) return data;
  if (data instanceof Uint8Array) return Buffer.from(data.buffer, data.byteOffset, data.byteLength);
  throw new TypeError(`${deserializer} takes a Buffer or null, not ${kind(data)}`);
}

/**
 * The canonical text of a UUID, in either case: eight, four, four, four and
 * twelve hexadecimal digits, joined by `-`.
 */
export const CANONICAL_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
