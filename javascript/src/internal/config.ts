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
// kafka_Map_t carries (private). The binding has no `ConfigDef` table and
// checks no value (root CLAUDE.md, JavaScript Binding Conventions,
// Configuration): the core checks types, ranges, enums, unknown keys and
// cross-key rules, with Java's messages that name the key.
//
// - The configuration is a plain object: a `Map` throws `TypeError`, since its
//   entries aren't object keys, and so does another non-plain object.
// - A key that is not a string (a symbol) throws `ConfigError` "Key must be a
//   string." (`Utils.castToStringObjectMap`): it has no string to cross as.
// - `null` and `undefined` leave the key unset, so the core's default applies.
// - Every value becomes the string the FFI takes: `true` as `'true'`, an
//   array's items joined with commas, a number in decimal (never in exponent
//   form, `1e21` as `'1000000000000000000000'`), a bigint in decimal (a long
//   above 2^53 goes as its exact digits), anything else by `String()` (an
//   object as `[object Object]`), for the core to check.
// - Unknown keys pass, and the core lists the ones it does not use.
// - `key.serializer` and `value.serializer` throw `ConfigError`, whatever
//   their value, until their route is built: a serializer is a constructor
//   argument (`keySerializer`, `valueSerializer`), never a configuration value
//   *(deviation. Why: JavaScript cannot load a class by name synchronously)*.
// - Keys and values cross as bytes, encoded once per client by
//   `encodeLikeJava` (src/internal/encode.ts): a lone surrogate becomes `?`,
//   as Java's UTF-8 encoding writes it.

import { ConfigError } from '../common/config/config_error';
import { encodeLikeJava } from './encode';

function typeName(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

/** A value as Java's `String.valueOf` writes it in a ConfigException message. */
function shown(value: unknown): string {
  if (value === null || value === undefined) return 'null';
  try {
    return configValue(value);
  } catch {
    return typeName(value);
  }
}

/** The constructor argument of a key whose value is a serializer, or null for another key. */
function serializerArgument(key: string): string | null {
  if (key === 'key.serializer') return 'keySerializer';
  if (key === 'value.serializer') return 'valueSerializer';
  return null;
}

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
 * One config value as the string the FFI takes: `true` as `'true'`, numbers
 * and bigints in decimal, an array's items each written so and joined with
 * commas, anything else by `String()`.
 */
function configValue(value: unknown): string {
  switch (typeof value) {
    case 'string':
      return value;
    case 'number':
      return decimal(value);
    case 'boolean':
    case 'bigint':
      return String(value);
    default:
      if (Array.isArray(value)) return value.map(configValue).join(',');
      // Any other value becomes text, an object `[object Object]`, for the
      // core to check (root CLAUDE.md, JavaScript Binding Conventions,
      // Configuration).
      return String(value);
  }
}

/**
 * The `[key, value]` byte pairs of `configs` (`encodeLikeJava`'s bytes of the
 * strings), a plain object of Java's producer config keys, each value written
 * as a string. `null` and `undefined` values are dropped, as an absent key. A
 * `Map` (or any other object that is not a plain one) is refused, so that its
 * entries are not silently ignored.
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
  // Every key first, as Java's Utils.castToStringObjectMap runs before
  // ConfigDef.parse: String(key), as a template literal throws on a Symbol.
  for (const [key, value] of entries) {
    if (typeof key !== 'string') {
      throw new ConfigError(`Invalid value ${shown(value)} for configuration ${String(key)}: Key must be a string.`);
    }
  }
  const out: Array<[Buffer, Buffer]> = [];
  for (const [key, value] of entries as Array<[string, unknown]>) {
    const argument = serializerArgument(key);
    if (argument !== null && value !== undefined) {
      throw new ConfigError(
        `${key} is not a configuration of this client: a serializer is the ${argument} argument of the ` +
          'constructor, never a configuration value',
      );
    }
    if (value === null || value === undefined) continue;
    // Encoded once per client, as Java encodes a string (src/internal/encode.ts).
    out.push([encodeLikeJava(key), encodeLikeJava(configValue(value))]);
  }
  return out;
}
