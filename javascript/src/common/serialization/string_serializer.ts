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

// Java's StringSerializer, as the built-in factory stringSerializer() (root
// CLAUDE.md, JavaScript Binding Conventions, Serialization): a serializer that
// writes a string as Java's String.getBytes(encoding) does, through
// encodeLikeJava (src/internal/encode.ts), each lone surrogate as `?` (Types,
// Q24). The factory takes an optional { configs, isKey }, Java's
// configure(configs, isKey), so that it also goes in a serde config key
// without `()` (Q53).

import { JavaForms, kind } from '../../internal/args';
import { encodeLikeJava, type JavaCharset, javaCharsetForName } from '../../internal/encode';
import { SerializationError } from '../errors/serialization_error';
import type { Serializer } from './serializer';

const FORMS = new JavaForms('stringSerializer', [[], ['configs', 'isKey']]);

/**
 * Java's `StringSerializer.configure`: the encoding of `key.serializer.encoding`
 * (for a key) or `value.serializer.encoding`, else of `serializer.encoding`;
 * a value that is not a string is ignored, as Java ignores one that is not a
 * `String`, and so is none (UTF-8).
 *
 * @throws `SerializationError` "Unsupported encoding <name>" for a name that
 *   is not one of Java's standard charsets
 */
function configure(configs: Readonly<Record<string, unknown>>, isKey: boolean): JavaCharset {
  const propertyName = isKey ? 'key.serializer.encoding' : 'value.serializer.encoding';
  let encodingValue: unknown = configs[propertyName];
  if (encodingValue === null || encodingValue === undefined) encodingValue = configs['serializer.encoding'];
  if (typeof encodingValue !== 'string') return 'UTF-8';
  const encoding = javaCharsetForName(encodingValue);
  if (encoding === null) throw new SerializationError({ message: `Unsupported encoding ${encodingValue}` });
  return encoding;
}

/**
 * The serializer of Java's `StringSerializer`: String encoding defaults to
 * UTF8 and can be customized by setting the property
 * `key.serializer.encoding`, `value.serializer.encoding` or
 * `serializer.encoding`. The first two take precedence over the last.
 *
 * @returns a serializer of strings, which writes a null string as null
 *
 * @remarks *(js-only)* The built-in factory of Java's class, which a
 * constructor argument takes called, `keySerializer: stringSerializer()`
 * (without `()` it throws "did you forget '()'?"), and a serde config key
 * without `()`, `'key.serializer': stringSerializer`. It writes Java's six
 * standard charsets (`US-ASCII`, `ISO-8859-1`, `UTF-8`, `UTF-16BE`,
 * `UTF-16LE`, `UTF-16`, under Java's names and aliases, ignoring case) byte
 * for byte as Java does. *(deviation: Java takes any charset its runtime
 * provides; another name throws `SerializationError` "Unsupported encoding
 * <name>". Why: Node has no Java charset providers. Goes when: never needed
 * for the standard set.)*
 */
export function stringSerializer(): Serializer<string>;
/**
 * The serializer of Java's `StringSerializer`, configured as Java's
 * `configure(configs, isKey)` configures it.
 *
 * @param options - the configuration and whether the serializer is for keys
 * @returns a serializer of strings, which writes a null string as null
 * @throws `SerializationError` "Unsupported encoding <name>" when the
 *   configured encoding is not UTF-8
 * @throws `IllegalArgumentError` when the given parameters are not `{}` or
 *   `{configs, isKey}`
 * @throws `TypeError` when `configs` is not an object or `isKey` not a
 *   boolean
 */
export function stringSerializer(options: {
  /** The configuration: `key.serializer.encoding`, `value.serializer.encoding` and `serializer.encoding` are read. */
  configs: Readonly<Record<string, unknown>>;
  /** Whether the serializer is for keys. */
  isKey: boolean;
}): Serializer<string>;
export function stringSerializer(options?: unknown): Serializer<string> {
  let encoding: JavaCharset = 'UTF-8';
  if (FORMS.match(options, arguments.length) === 1) {
    const configs = FORMS.value(options, 'configs');
    const isKey = FORMS.value(options, 'isKey');
    if (configs === null || typeof configs !== 'object') throw new TypeError(`configs must be an object, not ${kind(configs)}`);
    if (typeof isKey !== 'boolean') throw new TypeError(`isKey must be a boolean, not ${kind(isKey)}`);
    encoding = configure(configs as Readonly<Record<string, unknown>>, isKey);
  }
  return {
    serialize(given: { topic: string; data: string | null }): Uint8Array | null {
      const data: unknown = given.data;
      if (data === null) return null;
      if (typeof data !== 'string') throw new TypeError(`stringSerializer: data must be a string or null, not ${kind(data)}`);
      return encodeLikeJava(data, encoding);
    },
  };
}
