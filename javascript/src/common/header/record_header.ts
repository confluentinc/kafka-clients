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

// `RecordHeader` returns Node's Buffer: the directive carries @types/node into
// the compiled record_header.d.ts (`preserve`, as tsc >= 5.5 drops it from
// declarations otherwise).
/// <reference types="node" preserve="true" />

// Java's RecordHeader (org.apache.kafka.common.header.internals), the class an
// app builds a header with (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Serialization): a plain class holding the key and the value it
// is given, translated from the Java source. It is not a data class, so it
// keeps Java's methods, `key()` and `value()`.

import { JavaForms } from '../../internal/args';
import { bytesToString } from '../../internal/bytes';
import { bytesEquals, bytesHashCode, objectsEquals, sameClass, stringHashCode } from '../../internal/java_object';
import type { Header } from './header';

// Java's (String, byte[]) constructor; the (ByteBuffer, ByteBuffer) one is
// left out, as JavaScript has no ByteBuffer.
const FORMS = new JavaForms('RecordHeader', [['key', 'value']], {
  undefinedMessages: { value: "value must not be undefined. Pass null for Java's null" },
});
const KEY_FORMS = new JavaForms('key', [[]], { reader: true });
const VALUE_FORMS = new JavaForms('value', [[]], { reader: true });
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * A header value as the header keeps it: Java's `byte[]`, a `Buffer` over the
 * memory of the bytes given, never a copy, or null. A string is refused, as
 * Java's and Python's header values are bytes: encode it first.
 *
 * @param value - the value given
 * @returns a Buffer over the same memory, or null
 */
function valueOf(value: unknown): Buffer | null {
  if (value === null) return null;
  if (value instanceof Uint8Array) {
    return Buffer.isBuffer(value) ? value : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  }
  if (typeof value === 'string') {
    throw new TypeError(
      "value must be a Buffer, a Uint8Array or null, not a string: a header value is bytes, so encode it first (Buffer.from(value, 'utf8'))",
    );
  }
  throw new TypeError('value must be a Buffer, a Uint8Array or null');
}

/**
 * A header of a key and a value, the class that builds a {@link Header}.
 */
export class RecordHeader implements Header {
  readonly #key: string;
  readonly #value: Buffer | null;

  /**
   * Creates a header of a key and a value.
   *
   * @param options - the header's key and value
   * @throws `TypeError` when the key is null ("Null header keys are not
   *   permitted") or not a string, or the value is neither bytes nor null (a
   *   string included: encode it first) or is `undefined`
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{key, value}`
   *
   * @remarks The header keeps the value it is given, as Java's keeps its
   * array: a later write to those bytes changes the header's value.
   */
  constructor(options: {
    /** The header's key; must not be null. */
    key: string;
    /** The header's value, a `Buffer` or `Uint8Array`; may be null. */
    value: Uint8Array | null;
  }) {
    FORMS.match(options, arguments.length);
    const key = FORMS.value(options, 'key');
    // Java's Objects.requireNonNull(key, "Null header keys are not permitted"), before the value is read.
    if (key === null) throw new TypeError('Null header keys are not permitted');
    if (typeof key !== 'string') throw new TypeError('key must be a string');
    this.#value = valueOf(FORMS.value(options, 'value'));
    this.#key = key;
  }

  key(): string {
    KEY_FORMS.match(undefined, arguments.length);
    return this.#key;
  }

  /**
   * @remarks The value the constructor was given, as a `Buffer` over the
   * same memory, the same `Buffer` on every call.
   */
  value(): Buffer | null {
    VALUE_FORMS.match(undefined, arguments.length);
    return this.#value;
  }

  /**
   * Whether `o` is a header of this class with an equal key and the same value
   * bytes, Java's `RecordHeader.equals`: `Objects.equals` on the keys and
   * `Arrays.equals` on the values.
   *
   * @param options - the object to compare with
   * @returns true when `o` is an equal header
   * @throws `IllegalArgumentError` when the given parameters are not `{o}`
   */
  equals(options: {
    /** The object to compare with. */
    o: unknown;
  }): boolean;
  /**
   * The same as `equals({o})`, with the object on its own; a plain object is
   * read as the options object.
   *
   * @param o - the object to compare with
   * @returns true when `o` is an equal header
   */
  equals(o: unknown): boolean;
  equals(options: unknown): boolean {
    EQUALS_FORMS.match(options, arguments.length);
    const o = EQUALS_FORMS.value(options, 'o');
    if (this === o) return true;
    if (!sameClass(this, o)) return false;
    const header = o as RecordHeader;
    return objectsEquals(this.key(), header.key()) && bytesEquals(this.value(), header.value());
  }

  /**
   * The header's hash code, Java's `RecordHeader.hashCode`: the key's
   * `String.hashCode()`, then `31 * result + Arrays.hashCode(value)`.
   *
   * @returns the hash code, a 32-bit integer
   * @throws `TypeError` when it is given an argument
   */
  hashCode(): number {
    HASH_CODE_FORMS.match(undefined, arguments.length);
    let result = stringHashCode(this.key());
    result = (Math.imul(31, result) + bytesHashCode(this.value())) | 0;
    return result;
  }

  /**
   * The header as Java's `RecordHeader.toString()` writes it:
   * `RecordHeader(key = <key>, value = <the value's signed bytes, or null>)`.
   *
   * @returns the header's text
   */
  toString(): string {
    return `RecordHeader(key = ${this.key()}, value = ${bytesToString(this.value())})`;
  }

  /**
   * The header as `JSON.stringify` writes it: `{ key, value }`, the value a
   * `Buffer` (which writes itself as `{ type: 'Buffer', data }`), or null. It
   * ignores the key `JSON.stringify` passes.
   *
   * @returns the header's key and value
   *
   * @remarks *(js-only)* JavaScript's JSON protocol: without it the header,
   * whose key and value are private fields, would be written as `{}`.
   */
  toJSON(): { key: string; value: Buffer | null } {
    return { key: this.#key, value: this.#value };
  }
}
