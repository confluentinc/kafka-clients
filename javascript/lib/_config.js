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
// kafka_Map_t carries (private). Only the form changes here: the Rust core
// parses and validates every value, as Java's ConfigDef does.

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

function typeName(value) {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

/**
 * The `[key, value]` string pairs of `configs`, a plain object or a `Map` of
 * Java's config keys. `null` and `undefined` values are dropped, as an absent
 * key.
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
    if (value === null || value === undefined) continue;
    out.push([key, configValue(key, value)]);
  }
  return out;
}

module.exports = { configEntries };
