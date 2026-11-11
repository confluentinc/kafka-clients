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

// A producer's configuration object -> the `[key, value]` strings the C FFI's
// kafka_Map_t carries (private): the parts of Java's `ConfigDef` the client
// runs before the core sees the configuration, as
// python/confluent_kafka/_config.py's `prepare` does.
//
// - A value of a key Java's `ProducerConfig` defines is checked against the
//   key's `ConfigDef` type (`src/internal/config_types.ts`, generated from the Java
//   sources), as `ConfigDef.parseType` checks the object's type: a wrong
//   JavaScript type throws `ConfigError` with Java's message. A string is
//   accepted for every type but `STRING` / `PASSWORD`'s own check, since the
//   core parses and range-checks the text.
// - A `null` for a key whose validator rejects `null` throws `ConfigError` with
//   that validator's message, as `ConfigDef.parseValue` runs the validator on
//   `null`; any other `null`, and every `undefined`, is dropped (the key is not
//   set, so the core's default applies).
// - A key that is not a string (a symbol) throws `ConfigError` "Key must be a
//   string." (`Utils.castToStringObjectMap`).
// - The configuration is a plain object: a `Map` throws `TypeError`, as
//   Python's `dict` check refuses another type.
// - Every other key reaches the core as given (stringified), and the core
//   checks the values themselves (ranges, enums, boolean and number text) and
//   reports the keys it does not know.
// - `key.serializer` and `value.serializer` throw `ConfigError`, whatever
//   their value: a serializer is a constructor argument (`keySerializer`,
//   `valueSerializer`), never a configuration value *(deviation. Why:
//   JavaScript cannot load a class by name synchronously)*.
// - A `kafkaJS` block (the KafkaJS-compatible API's configuration) fails the
//   value check with a pointer to the migration guide.
// - The value of a key Java's `ProducerConfig` does not define must be a
//   string, a number or a boolean (`ConfigError` otherwise).
// - Values are written as the strings the core parses: `true` as `'true'`,
//   arrays joined with commas, numbers in decimal (`1e21` as
//   `'1000000000000000000000'`).
// - Keys and values cross as bytes, encoded once per client by
//   `encodeLikeJava` (src/internal/encode.ts): a lone surrogate becomes `?`,
//   as Java's UTF-8 encoding writes it.
//
// *(Deviation from Java and from the Python client:)* a `PASSWORD` value of the
// wrong type is not echoed in the message: it reads `[hidden]`, as Java's
// `Password.toString()`.

import { ConfigError } from '../common/config/config_error';
import { PRODUCER, PRODUCER_NULL } from './config_types';
import { encodeLikeJava } from './encode';

const INT_LABELS = new Map<string, string>([
  ['INT', 'a 32-bit integer'],
  ['SHORT', 'a 16-bit integer (short)'],
  ['LONG', 'a 64-bit integer (long)'],
]);

/** What Java's `Password.toString()` returns: a password is never shown. */
const HIDDEN = '[hidden]';

function typeName(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

/** A value as Java's `String.valueOf` writes it in a ConfigException message. */
function javaString(value: unknown): string {
  if (value === null || value === undefined) return 'null';
  if (Array.isArray(value)) return `[${value.map(javaString).join(', ')}]`;
  try {
    // eslint-disable-next-line @typescript-eslint/no-base-to-string -- Java's String.valueOf writes any object by its toString(), as ConfigException's message does (javascript/CLAUDE.md "Configuration")
    return String(value);
  } catch {
    return typeName(value);
  }
}

/**
 * Java's `ConfigException(name, value, message)`: "Invalid value <value> for
 * configuration <name>: <message>" (`value` `[hidden]` for a password).
 */
function invalidValue(name: string, value: unknown, message: string | null, type: string | undefined): ConfigError {
  const shown = value !== null && type === 'PASSWORD' ? HIDDEN : javaString(value);
  return new ConfigError(`Invalid value ${shown} for configuration ${name}${message === null ? '' : `: ${message}`}`);
}

/**
 * The JavaScript-type half of `ConfigDef.parseType(name, value, type)`:
 * nothing when `value`'s type is one the key's `ConfigDef` type takes, else
 * `ConfigError` with Java's message.
 *
 * @param name - the key
 * @param value - not null or undefined
 * @param type - the key's `ConfigDef.Type`
 */
export function checkType(name: string, value: unknown, type: string): void {
  if (typeof value === 'string' && type !== 'STRING' && type !== 'PASSWORD') return;
  switch (type) {
    case 'BOOLEAN':
      if (typeof value === 'boolean') return;
      throw invalidValue(name, value, 'Expected value to be either true or false', type);
    case 'STRING':
    case 'PASSWORD':
      if (typeof value === 'string') return;
      throw invalidValue(name, value, `Expected value to be a string, but it was a ${typeName(value)}`, type);
    case 'INT':
    case 'SHORT':
    case 'LONG':
      if (typeof value === 'bigint' || (typeof value === 'number' && Number.isInteger(value))) return;
      throw invalidValue(name, value, `Expected value to be ${INT_LABELS.get(type)}, but it was a ${typeName(value)}`, type);
    case 'DOUBLE':
      if (typeof value === 'number' || typeof value === 'bigint') return;
      throw invalidValue(name, value, `Expected value to be a double, but it was a ${typeName(value)}`, type);
    case 'LIST':
      if (Array.isArray(value)) return;
      throw invalidValue(name, value, 'Expected a comma separated list.', type);
    case 'CLASS':
      // A class key names a Java class the core resolves: only its name crosses.
      throw invalidValue(name, value, 'Expected a Class instance or class name.', type);
    default:
      throw invalidValue(name, value, `Unknown type ${type}`, type);
  }
}

/**
 * The validator half of `ConfigDef.parseValue` for an explicit `null`: a key
 * whose validator rejects it throws `ConfigError` built from Java's arguments
 * (`Range`: "Value must be non-null"; `ValidString`: "String must be one of:
 * ..."; a `ValidList` that refuses nulls: "Configuration '<key>' values must
 * not be null.").
 */
export function checkNull(name: string): void {
  const rule = PRODUCER_NULL.get(name);
  if (rule === undefined) return;
  const [form, message] = rule;
  if (form === 'named') throw invalidValue(name, null, message, PRODUCER.get(name));
  throw new ConfigError(message);
}

/** The constructor argument of a key whose value is a serializer, or null for another key. */
function serializerArgument(key: string): string | null {
  if (key === 'key.serializer') return 'keySerializer';
  if (key === 'value.serializer') return 'valueSerializer';
  return null;
}

/** The key of the KafkaJS-compatible API's configuration block. */
const KAFKAJS_KEY = 'kafkaJS';

/**
 * `n` written in decimal, never in exponent form (`1e21` is
 * `'1000000000000000000000'`, `1e-7` is `'0.0000001'`): the text Java's
 * `ConfigDef` parses. `NaN` and the infinities are written as JavaScript
 * writes them, for the core to refuse.
 */
export function decimal(n: number): string {
  const text = String(n);
  if (!Number.isFinite(n) || !/e/i.test(text)) return text;
  const negative = n < 0;
  const [mantissa, exponentText] = (negative ? text.slice(1) : text).split(/e/i);
  const exponent = Number(exponentText);
  const [whole, fraction = ''] = mantissa.split('.');
  const digits = whole + fraction;
  const point = whole.length + exponent;
  let out: string;
  if (point <= 0) out = `0.${'0'.repeat(-point)}${digits}`;
  else if (point >= digits.length) out = digits + '0'.repeat(point - digits.length);
  else out = `${digits.slice(0, point)}.${digits.slice(point)}`;
  return negative ? `-${out}` : out;
}

/**
 * One config value as the string the core parses. `known` is whether Java's
 * `ProducerConfig` defines the key (its value already passed `checkType`):
 * the value of any other key is a string, a number or a boolean.
 */
function configValue(key: string, value: unknown, known: boolean): string {
  switch (typeof value) {
    case 'string':
      return value;
    case 'boolean':
      return String(value);
    case 'number':
      return decimal(value);
    case 'bigint':
      if (known) return String(value);
      break;
    case 'object':
      if (known && Array.isArray(value)) return value.map((item) => configValue(key, item, false)).join(',');
      break;
    default:
      break;
  }
  throw invalidValue(key, value, `Expected value to be a string, a number or a boolean, but it was a ${typeName(value)}`, undefined);
}

/**
 * The `[key, value]` byte pairs of `configs` (`encodeLikeJava`'s bytes of the
 * strings), a plain object of Java's producer config keys, each value checked
 * against its key's `ConfigDef` type. `undefined` values, and `null` ones the key's validator accepts, are
 * dropped, as an absent key. A `Map` (or any other object that is not a plain
 * one) is refused, so that its entries are not silently ignored.
 */
export function configEntries(configs: unknown): Array<[Buffer, Buffer]> {
  // Java's constructor copies the map (new HashMap<>(configs)): a
  // NullPointerException for a null one.
  if (configs === null) throw new TypeError('configs must not be null');
  if (typeof configs !== 'object' || Array.isArray(configs)) {
    throw new TypeError(`configs must be a plain object, not ${typeName(configs)}`);
  }
  const proto: unknown = Object.getPrototypeOf(configs);
  if (proto !== Object.prototype && proto !== null) {
    throw new TypeError(`configs must be a plain object, not ${configs instanceof Map ? 'a Map' : 'a class instance'}`);
  }
  // The own enumerable keys, symbols included, so that a symbol key is
  // refused rather than skipped.
  const entries: Array<[unknown, unknown]> = Reflect.ownKeys(configs)
    .filter((key) => Object.prototype.propertyIsEnumerable.call(configs, key))
    .map((key) => [key, (configs as Record<PropertyKey, unknown>)[key]]);
  // Every key first, before any value is parsed, as Java's
  // Utils.castToStringObjectMap runs before ConfigDef.parse (and Python's
  // prepare checks the keys first): String(key), as a template literal throws
  // on a Symbol.
  for (const [key, value] of entries) {
    if (typeof key !== 'string') throw invalidValue(String(key), value, 'Key must be a string.', undefined);
  }
  const out: Array<[Buffer, Buffer]> = [];
  for (const [key, value] of entries as Array<[string, unknown]>) {
    if (value === undefined) continue;
    const argument = serializerArgument(key);
    if (argument !== null) {
      throw new ConfigError(
        `${key} is not a configuration of this client: a serializer is the ${argument} argument of the ` +
          'constructor, never a configuration value',
      );
    }
    if (value === null) {
      checkNull(key);
      continue;
    }
    if (key === KAFKAJS_KEY && typeof value === 'object') {
      throw invalidValue(
        key,
        value,
        'a kafkaJS block configures the KafkaJS-compatible API, which this client does not have: it takes ' +
          "Java's configuration keys (see MIGRATION.md, the guide to migrating from 1.x and KafkaJS)",
        undefined,
      );
    }
    const type = PRODUCER.get(key);
    if (type !== undefined) checkType(key, value, type);
    // Encoded once per client, as Java encodes a string (src/internal/encode.ts).
    out.push([encodeLikeJava(key), encodeLikeJava(configValue(key, value, type !== undefined))]);
  }
  return out;
}
