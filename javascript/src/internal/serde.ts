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

// The serdes a client is given (private): the check of the argument, the
// functions of the serde config keys, and the close at the client's close
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Serialization and
// Lifetime). One role for each client, the producer's serializers and the
// consumer's deserializers, shares the code.

import { kind } from './args';
import { isTypeFailure } from './builtin_serde';
import { DeserializerDefaults } from './deserializer_defaults';
import { SerializerDefaults } from './serializer_defaults';
import { ConfigError } from '../common/config/config_error';
import { KafkaError } from '../common/kafka_error';
import { SerializationError } from '../common/errors/serialization_error';
import type { Deserializer } from '../common/serialization/deserializer';
import type { Serializer } from '../common/serialization/serializer';

/** A serializer or a deserializer, as the binding holds it. */
export type Serde = Serializer<unknown> | Deserializer<unknown>;

/** What tells the producer's serializers from the consumer's deserializers. */
export interface SerdeRole {
  /** `producer` or `consumer`, for the messages. */
  readonly client: 'producer' | 'consumer';
  /** The client's class, which has the `create()` that awaits a Promise. */
  readonly clientClass: 'KafkaProducer' | 'KafkaConsumer';
  /** `serializer` or `deserializer`. */
  readonly noun: 'serializer' | 'deserializer';
  /** The serde's method, `serialize` or `deserialize`. */
  readonly method: 'serialize' | 'deserialize';
  /** The serde config keys, the key's then the value's. */
  readonly keys: readonly [string, string];
  /** The constructor arguments, the key's then the value's. */
  readonly args: readonly [string, string];
  /** The built-in class a hint names. */
  readonly example: string;
  /** Java's default `close()`, run for a serde without one. */
  readonly defaultClose: (serde: Serde) => void;
  /**
   * Whether Java's constructor takes the key's serde before it builds the
   * value's, so that a failure of the value's closes the key's, given or
   * made: `KafkaProducer` assigns its key serializer first, while
   * `AsyncKafkaConsumer` stores both deserializers only once both are built
   * (`Deserializers`), so a given one stays open.
   */
  readonly keyTakenFirst: boolean;
}

/** The producer's serializers. */
export const SERIALIZER: SerdeRole = Object.freeze({
  client: 'producer',
  clientClass: 'KafkaProducer',
  noun: 'serializer',
  method: 'serialize',
  keys: Object.freeze(['key.serializer', 'value.serializer'] as const),
  args: Object.freeze(['keySerializer', 'valueSerializer'] as const),
  example: 'StringSerializer',
  defaultClose: (serde: Serde) => SerializerDefaults.prototype.close.call(serde as Serializer<unknown>),
  keyTakenFirst: true,
});

/** The consumer's deserializers. */
export const DESERIALIZER: SerdeRole = Object.freeze({
  client: 'consumer',
  clientClass: 'KafkaConsumer',
  noun: 'deserializer',
  method: 'deserialize',
  keys: Object.freeze(['key.deserializer', 'value.deserializer'] as const),
  args: Object.freeze(['keyDeserializer', 'valueDeserializer'] as const),
  example: 'StringDeserializer',
  defaultClose: (serde: Serde) => DeserializerDefaults.prototype.close.call(serde as Deserializer<unknown>),
  keyTakenFirst: false,
});

/** The pair of serdes a client holds, the key's then the value's, null where none is. */
export type SerdePair = [Serde | null, Serde | null];

/**
 * The serde given as constructor argument `name` (`keySerializer`,
 * `valueDeserializer`, ...), or null for Java's null (or one not given): the
 * bytes then cross as they are.
 *
 * @param role - the client's role
 * @param value - the serde given, or null
 * @param name - the argument's name, for the error
 * @returns the serde, or null
 *
 * @throws `TypeError` for a function, which is no instance ("did you forget
 *   `new`?"), and any other value that is no serde
 */
export function checkSerde(role: SerdeRole, value: unknown, name: string): Serde | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'function') throw new TypeError(`${name}: pass an instance, not a function — did you forget \`new\`?`);
  if (typeof value !== 'object' || typeof (value as Record<string, unknown>)[role.method] !== 'function') {
    const noun = role.noun === 'serializer' ? 'Serializer' : 'Deserializer';
    throw new TypeError(`${name} must be a ${noun}: an object with a ${role.method}({ topic, headers, data }) method, or null`);
  }
  const close: unknown = (value as { close?: unknown }).close;
  if (close !== undefined && typeof close !== 'function') throw new TypeError(`${name}.close must be a function when given`);
  return value as Serde;
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

/** An identifier, which a hint may name a class by. */
const IDENTIFIER = /^[\p{ID_Start}$_][\p{ID_Continue}$‌‍]*$/u;

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
  let source = '';
  try {
    source = Function.prototype.toString.call(value);
  } catch {
    // A function whose source cannot be read names nothing from it.
  }
  const declared = /^class\s+([^\s{]+)/u.exec(source)?.[1];
  // `class extends Base {}` declares no name.
  if (declared !== undefined && declared !== 'extends' && IDENTIFIER.test(declared)) return declared;
  const inferred: unknown = Object.getOwnPropertyDescriptor(value, 'name')?.value;
  return typeof inferred === 'string' && IDENTIFIER.test(inferred) ? inferred : null;
}

/**
 * The class an object was built by, when it has a name, for a hint: a serde
 * object's `() => new X()`.
 *
 * @param value - the object
 * @returns its class name, or null for a plain object or an anonymous class
 */
function constructorName(value: object): string | null {
  try {
    const prototype: unknown = Object.getPrototypeOf(value);
    const constructor: unknown = prototype === null ? undefined : (prototype as { constructor?: unknown }).constructor;
    if (typeof constructor !== 'function' || constructor === Object) return null;
    return writtenClassName(constructor as (...args: never[]) => unknown);
  } catch {
    return null;
  }
}

/**
 * Whether `value` is a serde of `role`: an object or a function with the
 * role's method, and a `close` method or none.
 *
 * @param role - the client's role
 * @param value - the value to test
 * @returns whether it is a serde
 */
function isSerde(role: SerdeRole, value: unknown): value is Serde {
  if (value === null || (typeof value !== 'object' && typeof value !== 'function')) return false;
  const method: unknown = (value as Record<string, unknown>)[role.method];
  const close: unknown = (value as { close?: unknown }).close;
  return typeof method === 'function' && (close === undefined || typeof close === 'function');
}

/** The function a serde config key holds, `({ configs, isKey }) => serde`. */
type SerdeFunction = (options: { configs: Readonly<Record<string, unknown>>; isKey: boolean }) => unknown;

/** A serde config key's function, checked, and how to check what it made. */
type ConfiguredMaker = {
  /** Calls the function once with the configuration. */
  call: () => unknown;
  /** What it made, if a serde; else the `ConfigError` saying so. */
  check: (out: unknown) => Serde;
};

/**
 * The hint of a serde config key's `ConfigError`, naming class `name`, or
 * none.
 *
 * @param name - the class name, or null
 * @returns the hint, with its leading space
 */
function hint(name: string | null): string {
  return name === null ? '' : ` (did you mean \`() => new ${name}()\`?)`;
}

/**
 * The function the serde config key `index` (the key's 0, the value's 1) of
 * `configs` holds, checked, or null when the key is not set (`null` or
 * `undefined`): a function `({ configs, isKey }) => serde`, called once with
 * the whole configuration, in place of Java's class and its
 * `configure(configs, isKey)` (javascript/CLAUDE.md, JavaScript Binding
 * Conventions, Serialization). The configuration it gets is the client's,
 * with the `client.id` the parsed configuration generated, as Java configures
 * a serde with its `originals()` and that `client.id`.
 *
 * @param role - the client's role
 * @param configs - the client's configuration, as the function gets it
 * @param index - the key's (0) or the value's (1)
 * @param awaits - whether the function may return a Promise (`create()`)
 * @returns the checked function, or null
 *
 * @throws `ConfigError` when the value is a class name (a string), a serde
 *   object, a class or any other value that is no function
 */
function configuredMaker(
  role: SerdeRole,
  configs: Readonly<Record<string, unknown>>,
  index: 0 | 1,
  awaits: boolean,
): ConfiguredMaker | null {
  const key = role.keys[index];
  const value: unknown = configs[key];
  if (value === null || value === undefined) return null;
  const isKey = index === 0;
  const argument = role.args[index];
  const example = `() => new ${role.example}()`;
  const pass = awaits
    ? `pass a function ({ configs, isKey }) => ${role.noun}, or one returning a Promise of it, such as ${example}, ` +
      `or the ${role.noun} itself as the ${argument} argument`
    : `pass a synchronous function ({ configs, isKey }) => ${role.noun}, such as ${example}, ` +
      `or the ${role.noun} itself as the ${argument} argument of the constructor`;
  const invalid = (why: string): ConfigError =>
    new ConfigError(`Invalid value ${typeof value === 'string' ? value : kind(value)} for configuration ${key}: ${why}`);
  if (typeof value === 'string') {
    // A Java class name's last segment names the class a hint suggests.
    const simple = /[^.$]+$/u.exec(value)?.[0] ?? '';
    throw invalid(`a class name cannot be loaded in JavaScript${hint(IDENTIFIER.test(simple) ? simple : null)}; ${pass}`);
  }
  if (typeof value !== 'function') {
    if (isSerde(role, value)) throw invalid(`a ${role.noun} object is not a configuration value${hint(constructorName(value))}; ${pass}`);
    throw invalid(pass);
  }
  const make = value as SerdeFunction;
  if (isClass(make)) throw invalid(`a class is not called${hint(writtenClassName(make))}; ${pass}`);
  return {
    call: () => make({ configs, isKey }),
    check: (out) => {
      if (!isSerde(role, out)) {
        throw invalid(
          `the function returned ${kind(out)}, not a ${role.noun}: an object with a ${role.method}({ topic, headers, data }) method`,
        );
      }
      return out;
    },
  };
}

/**
 * Checks the serde config keys whose serde is not given: each holds no value
 * or a function, before the configuration is parsed and the functions are
 * called (`configuredMaker`).
 *
 * @param role - the client's role
 * @param configs - the client's configuration
 * @param given - the key's and the value's serdes given, or null
 * @param awaits - whether the functions may return a Promise (`create()`)
 *
 * @throws `ConfigError` as `configuredMaker`
 */
export function checkSerdeKeys(
  role: SerdeRole,
  configs: Readonly<Record<string, unknown>>,
  given: readonly [Serde | null, Serde | null],
  awaits: boolean,
): void {
  if (given[0] === null) configuredMaker(role, configs, 0, awaits);
  if (given[1] === null) configuredMaker(role, configs, 1, awaits);
}

/**
 * Java's relabelled construction failure, `KafkaException("Failed to
 * construct kafka producer", t)` (or `consumer`), for a serde config function
 * that threw or whose Promise rejected.
 *
 * @param role - the client's role
 * @param cause - what it threw or rejected with
 * @returns the error
 */
function constructionFailure(role: SerdeRole, cause: unknown): KafkaError {
  const error = new KafkaError({ message: `Failed to construct kafka ${role.client}`, cause: null });
  // The thrown value itself, whatever it is (`null` and `undefined` included).
  Object.defineProperty(error, 'cause', { value: cause, writable: true, enumerable: false, configurable: true });
  return error;
}

/**
 * The serde the serde config key `index` of `configs` makes, or null when the
 * key is not set, as the constructor takes it: its function must return the
 * serde itself, synchronously (`configuredMaker`).
 *
 * @param role - the client's role
 * @param configs - the client's configuration, as the function gets it
 * @param index - the key's (0) or the value's (1)
 * @returns the serde, or null
 *
 * @throws `ConfigError` when the value is no function, or the function
 *   returns a Promise, which only `create()` waits for, or no serde
 * @throws `KafkaError` "Failed to construct kafka producer" (or `consumer`),
 *   with the error as its `cause`, when the function throws, as Java wraps a
 *   serde that fails to build
 */
export function configuredSerde(role: SerdeRole, configs: Readonly<Record<string, unknown>>, index: 0 | 1): Serde | null {
  const maker = configuredMaker(role, configs, index, false);
  if (maker === null) return null;
  let out: unknown;
  try {
    out = maker.call();
  } catch (cause) {
    throw constructionFailure(role, cause);
  }
  if (isThenable(out)) {
    // Refused, so nobody else awaits it: the serde it resolves to is closed,
    // being nobody's, and its rejection is not an unhandled one.
    void Promise.resolve(out).then(
      (made: unknown) => {
        if (isSerde(role, made)) closeSerde(role, made, role.args[index]);
      },
      () => undefined,
    );
    throw new ConfigError(
      `Invalid value a function for configuration ${role.keys[index]}: the function returned a Promise, which the constructor cannot wait for; ` +
        `build the ${role.client} with await ${role.clientClass}.create(...), which waits for it`,
    );
  }
  return maker.check(out);
}

/**
 * The serdes the serde config keys make, key first, each function awaited
 * before the next is called, as `create()` takes them: a function may return
 * the serde or a Promise of it. A key whose serde is given is not read, as the
 * constructor ignores it. On a failure the serdes already taken are closed, as
 * the constructor closes them: the key's serde made when the value's function
 * fails, and, for the producer, a given one too (`keyTakenFirst`), unless the
 * value's config key is refused.
 *
 * @param role - the client's role
 * @param configs - the client's configuration, as the functions get it
 * @param given - the key's and the value's serdes given, or null
 * @returns the key's and the value's serdes made, null where none is
 *
 * @throws `ConfigError` (a rejection) as `configuredSerde`, except for a
 *   Promise, which is awaited
 * @throws `KafkaError` "Failed to construct kafka producer" (or `consumer`, a
 *   rejection), with the error as its `cause`, when a function throws or its
 *   Promise rejects
 */
export async function awaitedSerdes(
  role: SerdeRole,
  configs: Readonly<Record<string, unknown>>,
  given: readonly [Serde | null, Serde | null],
): Promise<SerdePair> {
  const made: SerdePair = [null, null];
  for (const index of [0, 1] as const) {
    if (given[index] !== null) continue;
    let out: unknown;
    try {
      const maker = configuredMaker(role, configs, index, true);
      if (maker === null) continue;
      try {
        out = await maker.call();
      } catch (cause) {
        throw constructionFailure(role, cause);
      }
      made[index] = maker.check(out);
    } catch (error) {
      // The value's failure: the key's serde made is closed, nobody else's;
      // the producer, whose Java constructor has taken the key's serde by
      // then (`keyTakenFirst`), closes a given one too on a construction
      // failure, but not on a ConfigError.
      const closesGiven = role.keyTakenFirst && !(error instanceof ConfigError);
      if (index === 1) closeSerde(role, closesGiven ? (given[0] ?? made[0]) : made[0], role.args[0]);
      throw error;
    }
  }
  return made;
}

/**
 * Whether `error`, which a client's construction threw, is one Java's
 * constructor relabels "Failed to construct kafka producer" (or `consumer`)
 * in the catch around its body, closing the serdes it had taken: a serde
 * config function's failure (`constructionFailure`) or the core's construction
 * failure, which carries that message. A `ConfigError`, which Java's parse
 * throws before that catch, is not.
 *
 * @param role - the client's role
 * @param error - what construction threw
 * @returns whether it is a construction failure
 */
export function isConstructionFailure(role: SerdeRole, error: unknown): boolean {
  return error instanceof KafkaError && error.constructor === KafkaError && error.message === `Failed to construct kafka ${role.client}`;
}

/**
 * The class name of a value, as Java's `getClass().getName()` names it in
 * `KafkaProducer#doSend`'s message, spelled for JavaScript: a value's
 * constructor name (`Buffer`, `StringSerializer`), a primitive's wrapper
 * (`String`, `Number`), `Object` for an object without one, and `null` for
 * null. It never throws.
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
 * JavaScript's form of Java's `ClassCastException`, or a built-in
 * serializer's type failure, becomes a `SerializationError` with Java's
 * message and what was thrown as its cause; anything else is returned as it
 * is.
 *
 * @param thrown - what the serializer threw, or rejected with
 * @param isKey - whether it is the key serializer
 * @param data - the key or value it was given
 * @param serializer - the serializer
 * @returns the error to answer with
 */
export function serializerFailure(thrown: unknown, isKey: boolean, data: unknown, serializer: object): unknown {
  if (!(thrown instanceof TypeError) && !isTypeFailure(thrown)) return thrown;
  const field = isKey ? 'key' : 'value';
  return new SerializationError({
    message: `Can't convert ${field} of class ${className(data)} to class ${className(serializer)} specified in ${field}.serializer`,
    cause: thrown,
  });
}

/**
 * Whether `value` is a thenable, which the client awaits.
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
 * Closes a serde at the client's close, as Java's `closeQuietly`: a serde
 * without `close()` is closed by Java's default, which does nothing, and what
 * one throws, or the rejection of a Promise it returns, goes to
 * `process.emitWarning`, never to the caller.
 *
 * @param role - the client's role
 * @param serde - the serde, or null
 * @param name - the serde's name, for the warning
 */
export function closeSerde(role: SerdeRole, serde: Serde | null, name: string): void {
  if (serde === null) return;
  const warn = (thrown: unknown): void => {
    process.emitWarning(`Failed to close ${role.client} ${name}: ${describe(thrown)}`);
  };
  try {
    if (serde.close === undefined) {
      role.defaultClose(serde);
      return;
    }
    const returned: unknown = serde.close();
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
