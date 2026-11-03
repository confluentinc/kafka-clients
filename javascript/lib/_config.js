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

// A producer's configuration object -> the `[key, value]` strings the C FFI's
// kafka_Map_t carries (private): the parts of Java's `ConfigDef` the client
// runs before the core sees the configuration, as
// python/confluent_kafka/_config.py's `prepare` does.
//
// - A value of a key Java's `ProducerConfig` defines is checked against the
//   key's `ConfigDef` type (`lib/_config_types.js`, generated from the Java
//   sources), as `ConfigDef.parseType` checks the object's type: a wrong
//   JavaScript type throws `ConfigError` with Java's message. A string is
//   accepted for every type but `STRING` / `PASSWORD`'s own check, since the
//   core parses and range-checks the text.
// - A `null` for a key whose validator rejects `null` throws `ConfigError` with
//   that validator's message, as `ConfigDef.parseValue` runs the validator on
//   `null`; any other `null`, and every `undefined`, is dropped (the key is not
//   set, so the core's default applies).
// - Every other key reaches the core as given (stringified), and the core
//   checks the values themselves (ranges, enums, boolean and number text) and
//   reports the keys it does not know.
//
// *(Deviation from Java and from the Python client:)* a `PASSWORD` value of the
// wrong type is not echoed in the message: it reads `[hidden]`, as Java's
// `Password.toString()`.

const { ConfigError } = require('./common/config/config_error');
const { PRODUCER, PRODUCER_NULL } = require('./_config_types');

const INT_LABELS = new Map([
  ['INT', 'a 32-bit integer'],
  ['SHORT', 'a 16-bit integer (short)'],
  ['LONG', 'a 64-bit integer (long)'],
]);

/** What Java's `Password.toString()` returns: a password is never shown. */
const HIDDEN = '[hidden]';

function typeName(value) {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

/** A value as Java's `String.valueOf` writes it in a ConfigException message. */
function javaString(value) {
  if (value === null || value === undefined) return 'null';
  if (Array.isArray(value)) return `[${value.map(javaString).join(', ')}]`;
  try {
    return String(value);
  } catch {
    return typeName(value);
  }
}

/**
 * Java's `ConfigException(name, value, message)`: "Invalid value <value> for
 * configuration <name>: <message>" (`value` `[hidden]` for a password).
 */
function invalidValue(name, value, message, type) {
  const shown = value !== null && type === 'PASSWORD' ? HIDDEN : javaString(value);
  return new ConfigError(`Invalid value ${shown} for configuration ${name}${message === null ? '' : `: ${message}`}`);
}

/**
 * The JavaScript-type half of `ConfigDef.parseType(name, value, type)`:
 * nothing when `value`'s type is one the key's `ConfigDef` type takes, else
 * `ConfigError` with Java's message.
 *
 * @param {string} name
 * @param {unknown} value not null or undefined
 * @param {string} type the key's `ConfigDef.Type`
 */
function checkType(name, value, type) {
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
 *
 * @param {string} name
 */
function checkNull(name) {
  const rule = PRODUCER_NULL.get(name);
  if (rule === undefined) return;
  const [form, message] = rule;
  if (form === 'named') throw invalidValue(name, null, message, PRODUCER.get(name));
  throw new ConfigError(message);
}

/** One config value as the string the core parses. */
function configValue(key, value) {
  switch (typeof value) {
    case 'string':
      return value;
    case 'boolean':
    case 'number':
    case 'bigint':
      return String(value);
    case 'object':
      if (Array.isArray(value)) return value.map((item) => configValue(key, item)).join(',');
      break;
    default:
      break;
  }
  throw new TypeError(`config ${key} must be a string, a number, a boolean or an array of them, not ${typeName(value)}`);
}

/**
 * The `[key, value]` string pairs of `configs`, a plain object or a `Map` of
 * Java's producer config keys, each value checked against its key's
 * `ConfigDef` type. `undefined` values, and `null` ones the key's validator
 * accepts, are dropped, as an absent key.
 *
 * @param {Record<string, unknown> | Map<string, unknown>} configs
 * @returns {Array<[string, string]>}
 */
function configEntries(configs) {
  if (configs === null || typeof configs !== 'object' || Array.isArray(configs)) {
    throw new TypeError(`configs must be an object or a Map, not ${typeName(configs)}`);
  }
  const source = configs instanceof Map ? configs.entries() : Object.entries(configs);
  const out = [];
  for (const [key, value] of source) {
    if (typeof key !== 'string') throw new TypeError(`config keys must be strings, not ${typeName(key)}`);
    if (value === undefined) continue;
    if (value === null) {
      checkNull(key);
      continue;
    }
    const type = PRODUCER.get(key);
    if (type !== undefined) checkType(key, value, type);
    out.push([key, configValue(key, value)]);
  }
  return out;
}

module.exports = { checkNull, checkType, configEntries };
