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
// CLAUDE.md, JavaScript Binding Conventions, Serialization), used by
// KafkaProducer.

import { kind } from './args';
import { illegalArgument } from './errors';
import { ConfigError } from '../common/config/config_error';
import { KafkaError } from '../common/kafka_error';
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

/** Whether `value` is a class, which is no serde config function. */
function isClass(value: (...args: never[]) => unknown): boolean {
  try {
    return /^class[\s{]/.test(Function.prototype.toString.call(value));
  } catch {
    return false;
  }
}

/** Whether `value` is an object or a function with a `serialize` method, and a `close` method or none. */
function isSerializer(value: unknown): value is Serializer<unknown> {
  if (value === null || (typeof value !== 'object' && typeof value !== 'function')) return false;
  const { serialize, close } = value as { serialize?: unknown; close?: unknown };
  return typeof serialize === 'function' && (close === undefined || typeof close === 'function');
}

/**
 * The serializer the serde config key `key` (`key.serializer`,
 * `value.serializer`) of `configs` makes, or null when the key is not set
 * (`null` or `undefined`): its value is a synchronous function
 * `({ configs, isKey }) => serializer`, called once with the whole
 * configuration, in place of Java's class and its `configure(configs, isKey)`
 * (root CLAUDE.md, JavaScript Binding Conventions, Serialization, Q53). The
 * configuration it gets records each of its keys the function reads, into
 * `read`, so that the producer leaves those the core doesn't know out of the
 * configuration it sends to the core, as Java's `originals()` marks them
 * used. The configuration is the user's, without Java's computed
 * `client.id`, and the function runs before the core checks it
 * *(deviation: the core doesn't exist yet; javascript/CLAUDE.md)*.
 *
 * @throws `ConfigError` when the value is a class name (a string), a
 *   serializer object, a class or any other value that is no function, or
 *   the function returns a Promise or no serializer
 * @throws `KafkaError` "Failed to construct kafka producer", with the error
 *   as its `cause`, when the function throws, as Java wraps a serializer that
 *   fails to build
 */
export function configuredSerializer(
  configs: Readonly<Record<string, unknown>>,
  key: 'key.serializer' | 'value.serializer',
  read: Set<string>,
): Serializer<unknown> | null {
  const value: unknown = configs[key];
  if (value === null || value === undefined) return null;
  const isKey = key === 'key.serializer';
  const argument = isKey ? 'keySerializer' : 'valueSerializer';
  const pass =
    'pass a synchronous function ({ configs, isKey }) => serializer, such as stringSerializer, ' +
    `or the serializer itself as the ${argument} argument of the constructor`;
  const invalid = (why: string): ConfigError =>
    new ConfigError(`Invalid value ${typeof value === 'string' ? value : kind(value)} for configuration ${key}: ${why}`);
  if (typeof value === 'string') throw invalid(`a class name cannot be loaded in JavaScript; ${pass}`);
  if (typeof value !== 'function') {
    throw invalid(isSerializer(value) ? `a serializer object is not a configuration value; ${pass}` : pass);
  }
  const make = value as (options: { configs: Readonly<Record<string, unknown>>; isKey: boolean }) => unknown;
  if (isClass(make)) throw invalid(`a class is not called; ${pass}`);
  // Java's RecordingMap: a key the function reads is marked used.
  const recording = new Proxy(configs, {
    get(target, property, receiver): unknown {
      if (typeof property === 'string' && Object.hasOwn(target, property)) read.add(property);
      return Reflect.get(target, property, receiver) as unknown;
    },
  });
  let out: unknown;
  try {
    out = make({ configs: recording, isKey });
  } catch (cause) {
    throw new KafkaError({ message: 'Failed to construct kafka producer', cause });
  }
  if (isThenable(out)) {
    // Refused, so nobody awaits it: its rejection is not an unhandled one.
    void Promise.resolve(out).then(undefined, () => undefined);
    throw invalid('the function returned a Promise; it must return the serializer itself, synchronously');
  }
  if (!isSerializer(out)) {
    throw invalid(`the function returned ${kind(out)}, not a serializer: an object with a serialize({ topic, headers, data }) method`);
  }
  return out;
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
 * The bytes a serializer gave, as the producer hands them over (borrowed
 * until the hand-over, like the app's bytes): a `Uint8Array` (a `Buffer` is
 * one) or null.
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
      SerializerDefaults.prototype.close.call(serializer);
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
