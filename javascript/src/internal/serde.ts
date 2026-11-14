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

// The serializers a producer is given (private): the check of the argument,
// one serialize() call, and the close at the producer's close (root
// CLAUDE.md, JavaScript Binding Conventions, Serialization). Shared by
// KafkaProducer and MockProducer.

import { kind } from './args';
import { illegalArgument } from './errors';
import type { Serializer } from '../common/serialization/serializer';
import { SerializerDefaults } from './serializer_defaults';

/**
 * The serializer given as `name` (`keySerializer`, `valueSerializer`), or
 * null for Java's null (or one not given): the bytes the record holds then
 * cross as they are.
 *
 * @throws `IllegalArgumentError` for a function that is no serializer, the
 *   factory rather than what it returns (Python's hint)
 * @throws `TypeError` for any other value that is no serializer
 */
export function checkSerializer(value: unknown, name: string): Serializer<unknown> | null {
  if (value === null || value === undefined) return null;
  const serialize: unknown =
    typeof value === 'object' || typeof value === 'function' ? (value as { serialize?: unknown }).serialize : undefined;
  if (typeof value === 'function' && typeof serialize !== 'function') {
    throw illegalArgument(`${name}: pass a serializer, not a function that makes one — did you forget '()'?`);
  }
  if ((typeof value !== 'object' && typeof value !== 'function') || typeof serialize !== 'function') {
    throw new TypeError(`${name} must be a Serializer: an object with a serialize({ topic, headers, data }) method, or null`);
  }
  const close: unknown = (value as { close?: unknown }).close;
  if (close !== undefined && typeof close !== 'function') throw new TypeError(`${name}.close must be a function when given`);
  return value as Serializer<unknown>;
}

/** Whether `value` is a thenable, which the producer awaits. */
export function isThenable(value: unknown): value is PromiseLike<unknown> {
  return (
    value !== null &&
    (typeof value === 'object' || typeof value === 'function') &&
    typeof (value as { then?: unknown }).then === 'function'
  );
}

/**
 * The bytes a serializer gave, as the producer hands them over (and copies):
 * a `Uint8Array` (a `Buffer` is one) or null.
 *
 * @throws `TypeError` for any other value
 */
export function serializedBytes(out: unknown, name: string): Uint8Array | null {
  if (out === null || out instanceof Uint8Array) return out;
  throw new TypeError(`${name}.serialize() must return a Uint8Array or null, not ${kind(out)}`);
}

/**
 * Closes a serializer at the producer's close, as Java's `closeQuietly`: a
 * serializer without `close()` is closed by doing nothing, and what one
 * throws, or the rejection of a Promise it returns, goes to
 * `process.emitWarning`, never to the caller.
 */
export function closeSerializer(serializer: Serializer<unknown> | null, name: string): void {
  if (serializer === null) return;
  const warn = (thrown: unknown): void => {
    process.emitWarning(`Failed to close producer ${name}: ${describe(thrown)}`);
  };
  try {
    if (serializer.close === undefined) {
      SerializerDefaults.close(serializer);
      return;
    }
    const returned: unknown = serializer.close();
    if (isThenable(returned)) void returned.then(undefined, warn);
  } catch (thrown) {
    warn(thrown);
  }
}

/** The text of a thrown value; it never throws. */
function describe(thrown: unknown): string {
  try {
    if (thrown instanceof Error) return `${String(thrown.name)}: ${String(thrown.message)}`;
    return String(thrown);
  } catch {
    return 'a value that cannot be converted to a string';
  }
}
