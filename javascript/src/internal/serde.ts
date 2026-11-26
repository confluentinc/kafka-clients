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
// one serialize() call, and the close at the producer's close
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Serialization), used by
// KafkaProducer.

import { kind } from './args';
import { illegalArgument } from './errors';
import { ConfigError } from '../common/config/config_error';
import { KafkaError } from '../common/kafka_error';
import { SerializationError } from '../common/errors/serialization_error';
import type { Serializer } from '../common/serialization/serializer';
import { SerializerDefaults } from './serializer_defaults';

/**
 * The serializer given as `name` (`keySerializer`, `valueSerializer`), or
 * null for Java's null (or one not given): the bytes the record holds then
 * cross as they are.
 *
 * @param value - the serializer given, or null
 * @param name - the argument's name, for the error
 * @returns the serializer, or null
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

/**
 * Whether `value` is a class, which is no serde config function.
 *
 * @param value - the function to test
 * @returns whether it is a class
 */
function isClass(value: (...args: never[]) => unknown): boolean {
  try {
    return /^class[\s{]/.test(Function.prototype.toString.call(value));
  } catch {
    return false;
  }
}

/**
 * The name a class is written under: the one its declaration gives
 * (`class MySerializer {}`), else the one JavaScript inferred from a binding
 * (`const MySerializer = class {}`) when it is an identifier, or null for an
 * anonymous class (`{ 'key.serializer': class {} }` infers the key, which is
 * no class name).
 *
 * @param value - the class
 * @returns its name, or null
 */
function writtenClassName(value: (...args: never[]) => unknown): string | null {
  const identifier = /^[\p{ID_Start}$_][\p{ID_Continue}$\u200c\u200d]*$/u;
  let source = '';
  try {
    source = Function.prototype.toString.call(value);
  } catch {
    // A function whose source cannot be read names nothing from it.
  }
  const declared = /^class\s+([^\s{]+)/u.exec(source)?.[1];
  // `class extends Base {}` declares no name.
  if (declared !== undefined && declared !== 'extends' && identifier.test(declared)) return declared;
  const inferred: unknown = Object.getOwnPropertyDescriptor(value, 'name')?.value;
  return typeof inferred === 'string' && identifier.test(inferred) ? inferred : null;
}

/**
 * Whether `value` is an object or a function with a `serialize` method, and a `close` method or none.
 *
 * @param value - the value to test
 * @returns whether it is a serializer
 */
function isSerializer(value: unknown): value is Serializer<unknown> {
  if (value === null || (typeof value !== 'object' && typeof value !== 'function')) return false;
  const { serialize, close } = value as { serialize?: unknown; close?: unknown };
  return typeof serialize === 'function' && (close === undefined || typeof close === 'function');
}

/** The function a serde config key holds, `({ configs, isKey }) => serializer`. */
type SerializerFunction = (options: { configs: Readonly<Record<string, unknown>>; isKey: boolean }) => unknown;

/** A serde config key's function, checked, and how to call it and check what it made. */
type ConfiguredMaker = {
  /** Calls the function once with the recording configuration. */
  call: () => unknown;
  /** What it made, if a serializer; else the `ConfigError` saying so. */
  check: (out: unknown) => Serializer<unknown>;
};

/**
 * The function the serde config key `key` (`key.serializer`,
 * `value.serializer`) of `configs` holds, checked, or null when the key is
 * not set (`null` or `undefined`): a function `({ configs, isKey }) =>
 * serializer`, called once with the whole configuration, in place of Java's
 * class and its `configure(configs, isKey)` (javascript/CLAUDE.md,
 * JavaScript Binding Conventions, Serialization). The configuration it gets
 * records each of its keys the function reads, into `read`, so that the
 * producer leaves those the core doesn't know out of the configuration it
 * sends to the core, as Java's `originals()` marks them used. The
 * configuration is the user's, without Java's computed `client.id`, and the
 * function runs before the core checks it.
 *
 * @param configs - the producer's configuration
 * @param key - the serde config key
 * @param read - collects the keys the function reads
 * @param awaits - whether the function may return a Promise (`create()`)
 * @returns the checked function, or null
 *
 * @throws `ConfigError` when the value is a class name (a string), a
 *   serializer object, a class or any other value that is no function
 */
function configuredMaker(
  configs: Readonly<Record<string, unknown>>,
  key: 'key.serializer' | 'value.serializer',
  read: Set<string>,
  awaits: boolean,
): ConfiguredMaker | null {
  const value: unknown = configs[key];
  if (value === null || value === undefined) return null;
  const isKey = key === 'key.serializer';
  const argument = isKey ? 'keySerializer' : 'valueSerializer';
  const pass = awaits
    ? 'pass a function ({ configs, isKey }) => serializer, or one returning a Promise of it, such as stringSerializer, ' +
      `or the serializer itself as the ${argument} argument`
    : 'pass a synchronous function ({ configs, isKey }) => serializer, such as stringSerializer, ' +
      `or the serializer itself as the ${argument} argument of the constructor`;
  const invalid = (why: string): ConfigError =>
    new ConfigError(`Invalid value ${typeof value === 'string' ? value : kind(value)} for configuration ${key}: ${why}`);
  if (typeof value === 'string') throw invalid(`a class name cannot be loaded in JavaScript; ${pass}`);
  if (typeof value !== 'function') {
    throw invalid(isSerializer(value) ? `a serializer object is not a configuration value; ${pass}` : pass);
  }
  const make = value as SerializerFunction;
  if (isClass(make)) {
    const name = writtenClassName(make);
    const hint = name === null ? '' : ` (did you mean \`() => new ${name}()\`?)`;
    throw invalid(`a class is not called${hint}; ${pass}`);
  }
  // Java's RecordingMap: a key the function reads is marked used.
  const recording = new Proxy(configs, {
    get(target, property, receiver): unknown {
      if (typeof property === 'string' && Object.hasOwn(target, property)) read.add(property);
      return Reflect.get(target, property, receiver) as unknown;
    },
  });
  return {
    call: () => make({ configs: recording, isKey }),
    check: (out) => {
      if (!isSerializer(out)) {
        throw invalid(`the function returned ${kind(out)}, not a serializer: an object with a serialize({ topic, headers, data }) method`);
      }
      return out;
    },
  };
}

/**
 * Java's relabelled construction failure, `KafkaException("Failed to
 * construct kafka producer", t)`, for a serde config function that threw or
 * whose Promise rejected.
 *
 * @param cause - what it threw or rejected with
 * @returns the error
 */
function constructionFailure(cause: unknown): KafkaError {
  return new KafkaError({ message: 'Failed to construct kafka producer', cause });
}

/**
 * The serializer the serde config key `key` of `configs` makes, or null when
 * the key is not set, as the constructor takes it: its function must return
 * the serializer itself, synchronously (`configuredMaker`).
 *
 * @param configs - the producer's configuration
 * @param key - the serde config key
 * @param read - collects the keys the function reads
 * @returns the serializer, or null
 *
 * @throws `ConfigError` when the value is a class name (a string), a
 *   serializer object, a class or any other value that is no function, or
 *   the function returns a Promise, which only `create()` waits for, or no
 *   serializer
 * @throws `KafkaError` "Failed to construct kafka producer", with the error
 *   as its `cause`, when the function throws, as Java wraps a serializer that
 *   fails to build
 */
export function configuredSerializer(
  configs: Readonly<Record<string, unknown>>,
  key: 'key.serializer' | 'value.serializer',
  read: Set<string>,
): Serializer<unknown> | null {
  const maker = configuredMaker(configs, key, read, false);
  if (maker === null) return null;
  let out: unknown;
  try {
    out = maker.call();
  } catch (cause) {
    throw constructionFailure(cause);
  }
  if (isThenable(out)) {
    // Refused, so nobody else awaits it: the serializer it resolves to is
    // closed, being nobody's, and its rejection is not an unhandled one.
    const name = key === 'key.serializer' ? 'keySerializer' : 'valueSerializer';
    void Promise.resolve(out).then(
      (made: unknown) => {
        if (isSerializer(made)) closeSerializer(made, name);
      },
      () => undefined,
    );
    throw new ConfigError(
      `Invalid value a function for configuration ${key}: the function returned a Promise, which the constructor cannot wait for; ` +
        'build the producer with await KafkaProducer.create(...), which waits for it',
    );
  }
  return maker.check(out);
}

/**
 * The serializers the serde config keys make, key first, each function
 * awaited before the next is called, as `create()` takes them: a function may
 * return the serializer or a Promise of it. A key whose serializer is given
 * is not read, as the constructor ignores it. On a failure the serializers
 * already taken are closed, as the constructor closes them: the key's, made
 * or given, when the value's function fails, and only one made when the
 * value's config key is refused.
 *
 * @param configs - the producer's configuration
 * @param given - the key and the value serializers given, or null
 * @param read - collects the keys the functions read
 * @returns the key's and the value's serializers made, null where none is
 *
 * @throws `ConfigError` (a rejection) as `configuredSerializer`, except for a
 *   Promise, which is awaited
 * @throws `KafkaError` "Failed to construct kafka producer" (a rejection),
 *   with the error as its `cause`, when a function throws or its Promise
 *   rejects
 */
export async function awaitedSerializers(
  configs: Readonly<Record<string, unknown>>,
  given: readonly [Serializer<unknown> | null, Serializer<unknown> | null],
  read: Set<string>,
): Promise<[Serializer<unknown> | null, Serializer<unknown> | null]> {
  const made: [Serializer<unknown> | null, Serializer<unknown> | null] = [null, null];
  const keys = ['key.serializer', 'value.serializer'] as const;
  for (const index of [0, 1] as const) {
    if (given[index] !== null) continue;
    let maker: ConfiguredMaker | null = null;
    let out: unknown;
    try {
      maker = configuredMaker(configs, keys[index], read, true);
      if (maker === null) continue;
      try {
        out = await maker.call();
      } catch (cause) {
        throw constructionFailure(cause);
      }
      made[index] = maker.check(out);
    } catch (error) {
      // The key's serializer is taken once the value's turn comes: a
      // construction failure closes it, given or made (Java's constructor
      // catch); a ConfigError only the one made, nobody else's.
      if (index === 1) closeSerializer(error instanceof ConfigError ? made[0] : (given[0] ?? made[0]), 'keySerializer');
      throw error;
    }
  }
  return made;
}

// The name of a built-in serializer's factory, on the object it made
// (`stringSerializer()`'s), which a serializer's wrapped TypeError names as
// Java names the class.
const BUILT_IN_NAME: unique symbol = Symbol('built-in serializer');

/**
 * Marks `serde` as the built-in factory `name`'s, so that an error naming its
 * class names the factory.
 *
 * @param serde - the serializer the factory made
 * @param name - the factory's name
 * @returns `serde`
 */
export function nameBuiltIn<T extends object>(serde: T, name: string): T {
  Object.defineProperty(serde, BUILT_IN_NAME, { value: name });
  return serde;
}

/**
 * The class name of a value, as Java's `getClass().getName()` names it in
 * `KafkaProducer#doSend`'s message, spelled for JavaScript: a value's
 * constructor name (`Buffer`), a primitive's wrapper (`String`, `Number`),
 * `Object` for an object without one, `null` for null, and a built-in
 * serializer's factory name (`stringSerializer`). It never throws.
 *
 * @param value - the value
 * @returns its class name
 */
export function className(value: unknown): string {
  if (value === null) return 'null';
  switch (typeof value) {
    case 'undefined':
      return 'undefined';
    case 'string':
      return 'String';
    case 'number':
      return 'Number';
    case 'bigint':
      return 'BigInt';
    case 'boolean':
      return 'Boolean';
    case 'symbol':
      return 'Symbol';
    default:
      break;
  }
  try {
    const builtIn: unknown = (value as { [BUILT_IN_NAME]?: unknown })[BUILT_IN_NAME];
    if (typeof builtIn === 'string') return builtIn;
    const prototype: unknown = Object.getPrototypeOf(value);
    const constructor: unknown = prototype === null ? undefined : (prototype as { constructor?: unknown }).constructor;
    const name: unknown = typeof constructor === 'function' ? constructor.name : undefined;
    return typeof name === 'string' && name !== '' ? name : 'Object';
  } catch {
    return 'Object';
  }
}

/**
 * What `KafkaProducer#doSend` makes of a serializer's throw: a `TypeError`,
 * JavaScript's form of Java's `ClassCastException`, becomes a
 * `SerializationError` with Java's message and the `TypeError` as its cause;
 * anything else is returned as it is.
 *
 * @param thrown - what the serializer threw, or rejected with
 * @param isKey - whether it is the key serializer
 * @param data - the key or value it was given
 * @param serializer - the serializer
 * @returns the error to answer with
 */
export function serializerFailure(thrown: unknown, isKey: boolean, data: unknown, serializer: object): unknown {
  if (!(thrown instanceof TypeError)) return thrown;
  const field = isKey ? 'key' : 'value';
  return new SerializationError({
    message: `Can't convert ${field} of class ${className(data)} to class ${className(serializer)} specified in ${field}.serializer`,
    cause: thrown,
  });
}

/**
 * Whether `value` is a thenable, which the producer awaits.
 *
 * @param value - the value to test
 * @returns whether it is a thenable
 */
export function isThenable(value: unknown): value is PromiseLike<unknown> {
  return (
    value !== null && (typeof value === 'object' || typeof value === 'function') && typeof (value as { then?: unknown }).then === 'function'
  );
}

/**
 * The bytes a serializer gave, as the producer hands them over (borrowed
 * until the hand-over, like the app's bytes): a `Uint8Array` (a `Buffer` is
 * one) or null.
 *
 * @param out - what the serializer returned
 * @param name - the serializer's name, for the error
 * @returns the bytes, or null
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
 *
 * @param serializer - the serializer, or null
 * @param name - the serializer's name, for the warning
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

/**
 * The text of a thrown value; it never throws.
 *
 * @param thrown - the thrown value
 * @returns its text
 */
function describe(thrown: unknown): string {
  try {
    if (thrown instanceof Error) return `${String(thrown.name)}: ${String(thrown.message)}`;
    return String(thrown);
  } catch {
    return 'a value that cannot be converted to a string';
  }
}
