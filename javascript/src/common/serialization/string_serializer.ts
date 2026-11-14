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
import { encodeLikeJava } from '../../internal/encode';
import { SerializationError } from '../errors/serialization_error';
import type { Serializer } from './serializer';

const FORMS = new JavaForms('stringSerializer', [[], ['configs', 'isKey']]);

// Java's names of UTF-8, the canonical one and its aliases, which
// Charset.forName matches ignoring case.
const UTF_8_NAMES: ReadonlySet<string> = new Set(['utf-8', 'utf8', 'unicode-1-1-utf-8']);

/**
 * Java's `StringSerializer.configure`: the encoding of `key.serializer.encoding`
 * (for a key) or `value.serializer.encoding`, else of `serializer.encoding`;
 * a value that is not a string is ignored, as Java ignores one that is not a
 * `String`. Only UTF-8 is supported.
 *
 * @throws `SerializationError` "Unsupported encoding <name>" for any other
 *   encoding
 */
function configure(configs: Readonly<Record<string, unknown>>, isKey: boolean): void {
  const propertyName = isKey ? 'key.serializer.encoding' : 'value.serializer.encoding';
  let encodingValue: unknown = configs[propertyName];
  if (encodingValue === null || encodingValue === undefined) encodingValue = configs['serializer.encoding'];
  if (typeof encodingValue === 'string' && !UTF_8_NAMES.has(encodingValue.toLowerCase())) {
    throw new SerializationError({ message: `Unsupported encoding ${encodingValue}` });
  }
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
 * without `()`, `'key.serializer': stringSerializer`. A lone surrogate is
 * written as `?`, as Java's encoder writes it. *(deviation: Java takes any
 * charset. Why: Node's encoders write other charsets' bytes differently from
 * Java's, so only UTF-8 matches Java byte for byte. Goes when: the list of
 * serde factories is decided.)*
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
  if (FORMS.match(options, arguments.length) === 1) {
    const configs = FORMS.value(options, 'configs');
    const isKey = FORMS.value(options, 'isKey');
    if (configs === null || typeof configs !== 'object') throw new TypeError(`configs must be an object, not ${kind(configs)}`);
    if (typeof isKey !== 'boolean') throw new TypeError(`isKey must be a boolean, not ${kind(isKey)}`);
    configure(configs as Readonly<Record<string, unknown>>, isKey);
  }
  return {
    serialize(given: { topic: string; data: string | null }): Uint8Array | null {
      const data: unknown = given.data;
      if (data === null) return null;
      if (typeof data !== 'string') throw new TypeError(`stringSerializer: data must be a string or null, not ${kind(data)}`);
      return encodeLikeJava(data);
    },
  };
}
