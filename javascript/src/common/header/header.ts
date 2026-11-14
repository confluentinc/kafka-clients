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

// `Header` returns Node's Buffer: the directive carries @types/node into the
// compiled header.d.ts (`preserve`, as tsc >= 5.5 drops it from declarations
// otherwise).
/// <reference types="node" preserve="true" />

// Java's Header, with the constructor of its one concrete class, the internal
// RecordHeader (key, value). It is not a data class (root CLAUDE.md,
// JavaScript Binding Conventions, Class family: the data-class list does not
// name it), so it keeps Java's methods, `key()` and `value()`, over private
// fields.

import { JavaForms } from '../../internal/args';
import { bytesEquals, bytesHashCode, objectsEquals, sameClass, stringHashCode } from '../../internal/java_object';
import { bytesToString } from '../../internal/bytes';

const FORMS = new JavaForms('Header', [['key', 'value']], {
  undefinedMessages: { value: "value must not be undefined. Pass null for Java's null" },
});
const KEY_FORMS = new JavaForms('key', [[]], { reader: true });
const VALUE_FORMS = new JavaForms('value', [[]], { reader: true });
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * The checked key of a header: Java's `RecordHeader` refuses a null key with
 * a `NullPointerException`, a `TypeError` here.
 */
function checkKey(key: unknown): string {
  if (key === null) throw new TypeError('Null header keys are not permitted');
  if (typeof key !== 'string') throw new TypeError('key must be a string');
  return key;
}

/**
 * The checked value of a header: bytes (Java's `byte[]`) or null, as a
 * `Buffer` over the same memory (no copy: a `byte[]` the client returns is a
 * `Buffer`, root CLAUDE.md, JavaScript Binding Conventions, Types). A string
 * is refused, since Java's and Python's header values are bytes: encode it
 * first.
 */
function checkValue(value: unknown): Buffer | null {
  if (value === null) return null;
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof Uint8Array) return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  if (typeof value === 'string') {
    throw new TypeError(
      "value must be a Buffer, a Uint8Array or null, not a string: a header value is bytes, so encode it first (Buffer.from(value, 'utf8'))",
    );
  }
  throw new TypeError('value must be a Buffer, a Uint8Array or null');
}

/**
 * A header is a key-value pair.
 */
export class Header {
  readonly #key: string;
  readonly #value: Buffer | null;

  /**
   * Creates a header of a key and a value. The value is kept, not copied:
   * the producer copies it when the record is sent.
   *
   * @param options - the header's key and value
   * @throws `TypeError` when the key is null (Java's "Null header keys are
   *   not permitted") or not a string, the value is a string (a header value
   *   is bytes: encode it first) or neither bytes nor null, or either is
   *   `undefined`
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{key, value}`
   *
   * @remarks Java's `Header` is an interface whose one class,
   * `RecordHeader`, is internal; this is its `(String, byte[])` constructor.
   */
  constructor(options: {
    /** The header's key; must not be null. */
    key: string;
    /** The header's value, a `Buffer` or `Uint8Array`; may be null. */
    value: Uint8Array | null;
  }) {
    FORMS.match(options, arguments.length);
    this.#key = checkKey(FORMS.value(options, 'key'));
    this.#value = checkValue(FORMS.value(options, 'value'));
  }

  /**
   * The header's key.
   *
   * @returns the header's key; must not be null.
   * @throws `TypeError` when it is given an argument
   */
  key(): string {
    KEY_FORMS.match(undefined, arguments.length);
    return this.#key;
  }

  /**
   * The header's value.
   *
   * @returns the header's value; may be null.
   * @throws `TypeError` when it is given an argument
   *
   * @remarks The value is a `Buffer` over the bytes the header was built with,
   * not a copy.
   */
  value(): Buffer | null {
    VALUE_FORMS.match(undefined, arguments.length);
    return this.#value;
  }

  /**
   * The key of `header`, read from its private field: a method reassigned on
   * the instance from plain JavaScript is never called. Called by `Headers`.
   *
   * @internal
   */
  static internalKeyOf(header: Header): string {
    return header.#key;
  }

  /**
   * The value of `header`, read from its private field. Called at send.
   *
   * @internal
   */
  static internalValueOf(header: Header): Buffer | null {
    return header.#value;
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
    const header = o as Header;
    return objectsEquals(this.#key, header.#key) && bytesEquals(this.#value, header.#value);
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
    let result = stringHashCode(this.#key);
    result = (Math.imul(31, result) + bytesHashCode(this.#value)) | 0;
    return result;
  }

  /**
   * The header as Java's `RecordHeader.toString()` writes it:
   * `RecordHeader(key = <key>, value = <the value's signed bytes, or null>)`.
   *
   * @returns the header's text
   */
  toString(): string {
    return `RecordHeader(key = ${this.#key}, value = ${bytesToString(this.#value)})`;
  }

  /**
   * The header as `JSON.stringify` writes it: `{ key, value }`, the value a
   * `Buffer` (which writes itself as `{ type: 'Buffer', data }`), or null. It
   * ignores the key `JSON.stringify` passes.
   *
   * @returns the header's key and value
   *
   * @remarks *(js-only)* JavaScript's JSON protocol: without it the header,
   * held in private fields, would be written as `{}`.
   */
  toJSON(): { key: string; value: Buffer | null } {
    return { key: this.#key, value: this.#value };
  }
}
