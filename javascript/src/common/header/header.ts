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
// RecordHeader (key, value). It is not a data class (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Class family: the data-class list does not
// name it), so it keeps Java's methods, `key()` and `value()`.
//
// It wraps the C RecordHeader (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Serialization): a private handle object
// (native/headers.c), either an owned header (one the app made, or the
// binding's copy of a header read from a Headers that can still change) or a
// view of a header inside a read-only Headers, which keeps those headers
// alive (option 1, javascript/design/current/producer/DECISIONS.md, entry 21).
// The key and value are read through kafka_common_header_Header_key and
// kafka_common_header_Header_value once, on first use.

import { JavaForms } from '../../internal/args';
import { encodeLikeJava } from '../../internal/encode';
import { bytesEquals, bytesHashCode, objectsEquals, sameClass, stringHashCode } from '../../internal/java_object';
import { bytesToString } from '../../internal/bytes';
// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('../../internal/native');

const FORMS = new JavaForms('Header', [['key', 'value']], {
  undefinedMessages: { value: "value must not be undefined. Pass null for Java's null" },
});
const KEY_FORMS = new JavaForms('key', [[]], { reader: true });
const VALUE_FORMS = new JavaForms('value', [[]], { reader: true });
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * The key of a header as it crosses to the core: encodeLikeJava's bytes, or
 * null, which the core refuses with Java's `NullPointerException` "Null
 * header keys are not permitted", a `TypeError` here.
 *
 * @param key - the key given
 * @returns its bytes, or null
 */
function keyBytes(key: unknown): Uint8Array | null {
  if (key === null) return null;
  if (typeof key !== 'string') throw new TypeError('key must be a string');
  return encodeLikeJava(key);
}

/** The handle `Header.internalOf` hands its constructor, through the `WRAP` token. */
let wrapping: object | null = null;
/** What `Header.internalOf` passes the constructor instead of an options object. */
const WRAP = Object.freeze({});

/**
 * The checked value of a header: bytes (Java's `byte[]`) or null. A string is
 * refused, since Java's and Python's header values are bytes: encode it
 * first.
 *
 * @param value - the value given
 * @returns the value, or null
 */
function checkValue(value: unknown): Uint8Array | null {
  if (value === null || value instanceof Uint8Array) return value;
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
  // The native header (native/headers.c), whose finalizer drops it.
  readonly #native: object;
  // The key and value, read from the native header on first use.
  #key: string | undefined = undefined;
  #value: Buffer | null | undefined = undefined;

  /**
   * Creates a header of a key and a value.
   *
   * @param options - the header's key and value
   * @throws `TypeError` when the key is null (Java's "Null header keys are
   *   not permitted"), not a string or holds a NUL byte, the value is a string (a header value
   *   is bytes: encode it first) or neither bytes nor null, or either is
   *   `undefined`
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{key, value}`
   *
   * @remarks Java's `Header` is an interface whose one class,
   * `RecordHeader`, is internal; this is its `(String, byte[])` constructor.
   * *(js-only)* The header is a native `RecordHeader`, which copies the key
   * and the value: a later write to the given bytes does not change the
   * header, where Java's keeps the given array. *(deviation)* A key crosses
   * as its UTF-8 bytes, a lone surrogate as `?`, as Java's encoder writes
   * it, so `key()` reads `?` there, and a key holding a NUL is refused,
   * where Java's `RecordHeader` keeps the `String`. Why: the C API takes the
   * key as a NUL-terminated UTF-8 string. Goes when, for the NUL: the key
   * crosses the C API with a length.
   */
  constructor(options: {
    /** The header's key; must not be null. */
    key: string;
    /** The header's value, a `Buffer` or `Uint8Array`; may be null. */
    value: Uint8Array | null;
  }) {
    if ((options as unknown) === WRAP && wrapping !== null) {
      this.#native = wrapping;
      wrapping = null;
      return;
    }
    FORMS.match(options, arguments.length);
    const key = keyBytes(FORMS.value(options, 'key'));
    // Java's RecordHeader checks the key first: a null key is the core's
    // error, whatever the value.
    this.#native = native.headerNew(key, key === null ? null : checkValue(FORMS.value(options, 'value')));
  }

  /**
   * A header over a native header handle the library got from the addon (an
   * owned copy, or a view that keeps its headers alive).
   *
   * @param handle - the native header
   * @returns the header
   *
   * @internal
   */
  static internalOf(handle: object): Header {
    wrapping = handle;
    try {
      return new Header(WRAP as never);
    } finally {
      wrapping = null;
    }
  }

  /**
   * The native header of `header`, for `Headers.add`.
   *
   * @param header - the header
   * @returns its native handle
   *
   * @internal
   */
  static internalNativeOf(header: Header): object {
    return header.#native;
  }

  /**
   * A header value as the core takes it, checked as this constructor checks
   * it, for `Headers.add(key, value)`.
   *
   * @param value - the value given
   * @returns the value, or null
   *
   * @internal
   */
  static internalValueBytes(value: unknown): Uint8Array | null {
    return checkValue(value);
  }

  /**
   * The header's key.
   *
   * @returns the header's key; must not be null.
   * @throws `TypeError` when it is given an argument
   */
  key(): string {
    KEY_FORMS.match(undefined, arguments.length);
    return Header.internalKeyOf(this);
  }

  /**
   * The header's value.
   *
   * @returns the header's value; may be null.
   * @throws `TypeError` when it is given an argument
   *
   * @remarks *(js-only)* The value is a `Buffer` over the native header's
   * bytes, the same `Buffer` on every call, which keeps the header alive;
   * where the runtime refuses external buffers, a copy.
   */
  value(): Buffer | null {
    VALUE_FORMS.match(undefined, arguments.length);
    return Header.internalValueOf(this);
  }

  /**
   * The key of `header`, read from its native header once: a method
   * reassigned on the instance from plain JavaScript is never called.
   *
   * @param header - the header to read
   * @returns its key
   *
   * @internal
   */
  static internalKeyOf(header: Header): string {
    if (header.#key === undefined) header.#key = native.headerKey(header.#native);
    return header.#key;
  }

  /**
   * The value of `header`, read from its native header once.
   *
   * @param header - the header to read
   * @returns its value, or null
   *
   * @internal
   */
  static internalValueOf(header: Header): Buffer | null {
    if (header.#value === undefined) header.#value = native.headerValue(header.#native);
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
    return (
      objectsEquals(Header.internalKeyOf(this), Header.internalKeyOf(header)) &&
      bytesEquals(Header.internalValueOf(this), Header.internalValueOf(header))
    );
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
    let result = stringHashCode(Header.internalKeyOf(this));
    result = (Math.imul(31, result) + bytesHashCode(Header.internalValueOf(this))) | 0;
    return result;
  }

  /**
   * The header as Java's `RecordHeader.toString()` writes it:
   * `RecordHeader(key = <key>, value = <the value's signed bytes, or null>)`.
   *
   * @returns the header's text
   */
  toString(): string {
    return `RecordHeader(key = ${Header.internalKeyOf(this)}, value = ${bytesToString(Header.internalValueOf(this))})`;
  }

  /**
   * The header as `JSON.stringify` writes it: `{ key, value }`, the value a
   * `Buffer` (which writes itself as `{ type: 'Buffer', data }`), or null. It
   * ignores the key `JSON.stringify` passes.
   *
   * @returns the header's key and value
   *
   * @remarks *(js-only)* JavaScript's JSON protocol: without it the header,
   * a native handle in a private field, would be written as `{}`.
   */
  toJSON(): { key: string; value: Buffer | null } {
    return { key: Header.internalKeyOf(this), value: Header.internalValueOf(this) };
  }
}
