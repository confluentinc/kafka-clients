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

// Java's Headers interface joined with its class RecordHeaders
// (org.apache.kafka.common.header.internals), as Python's Headers is
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Serialization): a
// plain class holding its own list of headers, translated from the Java
// source, with the interface's methods, RecordHeaders' constructors, equals,
// hashCode and toString, and its read-only state. The core gets a copy of the
// headers only at send() (`internalToNative`).

import { JavaForms } from '../../internal/args';
import { encodeLikeJava } from '../../internal/encode';
import { illegalArgument, illegalState } from '../../internal/errors';
import { objectHashCode, objectsEquals, sameClass } from '../../internal/java_object';
import type { Header } from './header';
import { RecordHeader } from './record_header';

/**
 * Whether `value` is an object with a callable `[Symbol.iterator]` (an array, a `Headers`, a `Set`, ...).
 *
 * @param value - the value to test
 * @returns whether it is an iterable object
 */
function isIterableObject(value: unknown): boolean {
  return (
    (typeof value === 'object' || typeof value === 'function') &&
    value !== null &&
    typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === 'function'
  );
}

/**
 * Whether `value` is a {@link Header}: an object, not an array, with Java's
 * `key()` and `value()` methods, as Java takes any implementation of its
 * interface.
 *
 * @param value - the value to test
 * @returns whether it is a header
 */
function isHeader(value: unknown): value is Header {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false;
  const h = value as Partial<Header>;
  return typeof h.key === 'function' && typeof h.value === 'function';
}

/**
 * Whether `value` is a key, a string, or Java's null.
 *
 * @param value - the value to test
 * @returns whether it is a header key
 */
function isKey(value: unknown): boolean {
  return value === null || typeof value === 'string';
}

/**
 * The type of `value` as a message names it, as Python's `type(x).__name__`
 * does: a primitive's `typeof`, an array with its length, an object's class.
 *
 * @param value - the value
 * @returns its type's name
 */
function typeName(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return `Array(${value.length})`;
  if (typeof value !== 'object') return typeof value;
  const ctor = (value as { constructor?: unknown }).constructor;
  return typeof ctor === 'function' && ctor.name !== '' ? ctor.name : 'Object';
}

/**
 * One element of the headers given where Java takes `Iterable<Header>`: a
 * `Header` as it is, as Java's `RecordHeaders` keeps the object, and a
 * `[key, value]` pair as a new `RecordHeader`, checked with Python's messages
 * (javascript/CLAUDE.md, JavaScript Binding Conventions, Serialization). A
 * null element and a null key are Java's own `NullPointerException`s.
 *
 * @param item - the element
 * @param index - its position, for the messages
 * @returns the header
 */
function headerOf(item: unknown, index: number): Header {
  if (isHeader(item)) return item;
  if (item === null) throw new TypeError('Header cannot be null.');
  if (item === undefined) {
    throw new TypeError(`headers[${index}] must not be undefined. Pass a Header or a [key, value] pair, or leave it out`);
  }
  if (!Array.isArray(item) || item.length !== 2) {
    throw new TypeError(`headers[${index}] must be a Header or a [key, value] pair; got ${typeName(item)}`);
  }
  const key: unknown = item[0];
  const value: unknown = item[1];
  if (key === null) throw new TypeError('Null header keys are not permitted');
  if (typeof key !== 'string') throw new TypeError(`headers[${index}] key must be a string; got ${typeName(key)}`);
  if (value === undefined) throw new TypeError(`headers[${index}] value must not be undefined. Pass null for Java's null`);
  if (value !== null && !(value instanceof Uint8Array)) {
    throw new TypeError(`headers[${index}] value must be a Buffer, a Uint8Array or null; got ${typeName(value)}`);
  }
  return new RecordHeader({ key, value });
}

/**
 * The run-time type of a key, which TypeScript cannot enforce: a string, or
 * Java's null, which `checkKey` refuses.
 *
 * @param key - the key given
 * @returns the key
 */
function keyArg(key: unknown): string | null {
  if (key !== null && typeof key !== 'string') throw new TypeError('key must be a string');
  return key;
}

/**
 * Java's `RecordHeaders.checkKey`.
 *
 * @param key - the key given
 * @returns the key
 */
function checkKey(key: string | null): string {
  if (key === null) throw illegalArgument('key cannot be null.');
  return key;
}

// Java's forms, each one-parameter form's bare value picked by its type.
const CONSTRUCTOR_FORMS = new JavaForms('Headers', [[], ['headers']], {
  bareTypes: { headers: (value) => value === null || isIterableObject(value) },
});
const ADD_FORMS = new JavaForms('add', [['header'], ['key', 'value']], {
  undefinedMessages: { value: "value must not be undefined. Pass null for Java's null" },
  bareTypes: { header: (value) => value === null || isHeader(value) },
});
const REMOVE_FORMS = new JavaForms('remove', [['key']], { bareTypes: { key: isKey } });
const LAST_HEADER_FORMS = new JavaForms('lastHeader', [['key']], { bareTypes: { key: isKey } });
const HEADERS_FORMS = new JavaForms('headers', [['key']], { bareTypes: { key: isKey } });
const TO_ARRAY_FORMS = new JavaForms('toArray', [[]], { reader: true });
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });
const IS_READ_ONLY_FORMS = new JavaForms('isReadOnly', [[]], { reader: true });

/**
 * A mutable ordered collection of {@link Header} objects. Note that multiple
 * headers may have the same {@link Header.key | key}.
 *
 * The order of headers is preserved in the order they were added.
 *
 * @remarks *(deviation)* Also Java's internal `RecordHeaders`, whose
 * constructors are how Java users build headers.
 */
export class Headers implements Iterable<Header> {
  // Java's `List<Header> headers`: the Header objects given, in order.
  readonly #headers: Header[];
  // Java's `isReadOnly`.
  #isReadOnly = false;

  /**
   * Creates empty headers, Java's `RecordHeaders()`.
   */
  constructor();
  /**
   * Creates headers holding the given headers in their order, Java's
   * `RecordHeaders(Header[])`; null is no headers.
   *
   * @param options - the headers
   * @throws `TypeError` when a header is null ("Header cannot be null."), or
   *   an element is neither a `Header` nor a `[key, value]` pair
   *
   * @remarks *(deviation)* An element may also be a `[key, value]` pair,
   * which becomes a new `RecordHeader`. Why: a convenience for writing
   * headers, as in Python.
   */
  constructor(options: {
    /** The headers, an array of `Header` objects and `[key, value]` pairs, or null. */
    headers: readonly (Header | readonly [string, Uint8Array | null])[] | null;
  });
  /**
   * Creates headers holding the headers of an iterable in their order, Java's
   * `RecordHeaders(Iterable<Header>)`: a `Headers` is copied, its read-only
   * state not.
   *
   * @param options - the headers
   * @throws `TypeError` when `headers` is not iterable, or an element is not
   *   a `Header` or a `[key, value]` pair
   */
  constructor(options: {
    /** The headers, a `Headers` or an iterable of `Header` objects and `[key, value]` pairs, or null. */
    headers: Iterable<Header | readonly [string, Uint8Array | null]> | null;
  });
  /**
   * Creates headers holding the given headers in their order: the headers on
   * their own, the same as `{ headers }`, Java's `RecordHeaders(Header[])`.
   *
   * @param headers - The headers: an array of `Header` objects and `[key, value]` pairs, or null
   */
  constructor(headers: readonly (Header | readonly [string, Uint8Array | null])[] | null);
  /**
   * Creates headers holding the headers of an iterable in their order: the
   * headers on their own, the same as `{ headers }`, Java's
   * `RecordHeaders(Iterable<Header>)`.
   *
   * @param headers - The headers: a `Headers` or an iterable of `Header` objects and `[key, value]` pairs, or null
   */
  constructor(headers: Iterable<Header | readonly [string, Uint8Array | null]> | null);
  constructor(options?: unknown) {
    const given = CONSTRUCTOR_FORMS.match(options, arguments.length) === 0 ? null : CONSTRUCTOR_FORMS.value(options, 'headers');
    this.#headers = Headers.#taken(given);
  }

  /**
   * The list of new headers holding what `headers` holds, taken as Java's
   * RecordHeaders(Header[]) and RecordHeaders(Iterable<Header>) take them:
   * null is none, a `Headers` is copied, and each element is checked as it is
   * read, so nothing after a null one is read.
   *
   * @param headers - the headers given, or null
   * @returns the new list
   */
  static #taken(headers: unknown): Header[] {
    if (headers === null) return [];
    if (headers instanceof Headers) return [...headers.#headers];
    if (!isIterableObject(headers)) {
      throw new TypeError('headers must be a Headers, or an iterable of Header objects and [key, value] pairs');
    }
    const list: Header[] = [];
    for (const item of headers as Iterable<unknown>) list.push(headerOf(item, list.length));
    return list;
  }

  /**
   * Java's `RecordHeaders.canWrite`.
   */
  #canWrite(): void {
    if (this.#isReadOnly) throw illegalState('RecordHeaders has been closed.');
  }

  /**
   * Adds a header (key inside), to the end, returning if the operation
   * succeeded.
   *
   * @param options - the header
   * @returns this instance of the Headers, once the header is added.
   * @throws `IllegalStateError` is thrown if headers are in a read-only state.
   * @throws `TypeError` when the header is null ("Header cannot be null.") or
   *   not a `Header`
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{header}` or `{key, value}`
   */
  add(options: {
    /** The Header to be added. */
    header: Header;
    key?: never;
    value?: never;
  }): Headers;
  /**
   * Creates and adds a header, to the end, returning if the operation
   * succeeded.
   *
   * @param options - the header's key and value
   * @returns this instance of the Headers, once the header is added.
   * @throws `IllegalStateError` is thrown if headers are in a read-only state.
   * @throws `TypeError` when the key is null or not a string, or the value is
   *   neither bytes nor null (a string included: encode it first)
   */
  add(options: {
    header?: never;
    /** The key of the header to be added; must not be null. */
    key: string;
    /** The value of the header to be added; may be null. */
    value: Uint8Array | null;
  }): Headers;
  /**
   * Adds a header (key inside), to the end: a `RecordHeader` on its own, the
   * same as `add({header})`.
   *
   * @param header - the Header to be added.
   * @returns this instance of the Headers, once the header is added.
   *
   * @remarks A `Header` of another class goes by name, `add({ header })`,
   * since TypeScript cannot tell it from a plain object, the options object.
   */
  add(header: RecordHeader): Headers;
  add(options: unknown): Headers {
    let header: Header;
    if (ADD_FORMS.match(options, arguments.length) === 0) {
      const given = ADD_FORMS.value(options, 'header');
      // Java's Objects.requireNonNull(header, "Header cannot be null."), before canWrite().
      if (given === null) throw new TypeError('Header cannot be null.');
      if (!isHeader(given)) throw new TypeError('header must be a Header');
      header = given;
    } else {
      // Java's add(key, value) is add(new RecordHeader(key, value)): the
      // header's checks come before canWrite().
      header = new RecordHeader({
        key: ADD_FORMS.value(options, 'key') as string,
        value: ADD_FORMS.value(options, 'value') as Uint8Array | null,
      });
    }
    this.#canWrite();
    this.#headers.push(header);
    return this;
  }

  /**
   * Removes all headers for the given key returning if the operation
   * succeeded, while preserving the insertion order of the remaining headers.
   *
   * @param options - the key
   * @returns this instance of the Headers, once the header is removed.
   * @throws `IllegalStateError` is thrown if headers are in a read-only state.
   * @throws `IllegalArgumentError` when the key is null ("key cannot be
   *   null."), or the given parameters are not `{key}`
   * @throws `TypeError` when the key is not a string or is `undefined`
   */
  remove(options: {
    /** The key to remove all headers for; must not be null. */
    key: string;
  }): Headers;
  /**
   * Removes all headers for the given key: the key on its own, the same as
   * `remove({key})`.
   *
   * @param key - to remove all headers for; must not be null.
   * @returns this instance of the Headers, once the header is removed.
   */
  remove(key: string): Headers;
  remove(options: unknown): Headers {
    REMOVE_FORMS.match(options, arguments.length);
    const given = keyArg(REMOVE_FORMS.value(options, 'key'));
    // Java's canWrite() comes before checkKey(key).
    this.#canWrite();
    const key = checkKey(given);
    const list = this.#headers;
    let kept = 0;
    for (const header of list) if (header.key() !== key) list[kept++] = header;
    list.length = kept;
    return this;
  }

  /**
   * Returns just one (the very last) header for the given key, if present.
   *
   * @param options - the key
   * @returns this last header matching the given key, returns null if not
   *   present.
   * @throws `IllegalArgumentError` when the key is null ("key cannot be
   *   null."), or the given parameters are not `{key}`
   * @throws `TypeError` when the key is not a string or is `undefined`
   */
  lastHeader(options: {
    /** The key to get the last header for; must not be null. */
    key: string;
  }): Header | null;
  /**
   * Returns the very last header for the given key: the key on its own, the
   * same as `lastHeader({key})`.
   *
   * @param key - to get the last header for; must not be null.
   * @returns this last header matching the given key, returns null if not
   *   present.
   */
  lastHeader(key: string): Header | null;
  lastHeader(options: unknown): Header | null {
    LAST_HEADER_FORMS.match(options, arguments.length);
    const key = checkKey(keyArg(LAST_HEADER_FORMS.value(options, 'key')));
    const list = this.#headers;
    for (let i = list.length - 1; i >= 0; i--) if (list[i].key() === key) return list[i];
    return null;
  }

  /**
   * Returns all headers for the given key, in the order they were added in,
   * if present.
   *
   * @param options - the key
   * @returns all headers for the given key, in the order they were added in,
   *   if NO headers are present an empty iterable is returned.
   * @throws `IllegalArgumentError` when the key is null ("key cannot be
   *   null."), or the given parameters are not `{key}`
   * @throws `TypeError` when the key is not a string or is `undefined`
   */
  headers(options: {
    /** The key to return the headers for; must not be null. */
    key: string;
  }): Iterable<Header>;
  /**
   * Returns all headers for the given key: the key on its own, the same as
   * `headers({key})`.
   *
   * @param key - to return the headers for; must not be null.
   * @returns all headers for the given key, in the order they were added in.
   */
  headers(key: string): Iterable<Header>;
  headers(options: unknown): Iterable<Header> {
    HEADERS_FORMS.match(options, arguments.length);
    const key = checkKey(keyArg(HEADERS_FORMS.value(options, 'key')));
    const list = this.#headers;
    // Java's FilterByKeyIterator over the list's own iterator, made each time
    // the iterable is iterated.
    return {
      *[Symbol.iterator](): Iterator<Header> {
        for (const header of list) if (header.key() === key) yield header;
      },
    };
  }

  /**
   * Returns all headers as an array, in the order they were added in.
   *
   * @returns the headers as an array, mutating this array will not affect the
   *   Headers, if NO headers are present an empty array is returned.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  toArray(): Header[] {
    TO_ARRAY_FORMS.match(undefined, arguments.length);
    return [...this.#headers];
  }

  /**
   * The headers, in the order they were added in.
   *
   * @returns an iterator over the headers
   *
   * @remarks *(deviation)* A change made during an iteration is seen by it,
   * as by a JavaScript array's iterator, and throws nothing, where Java's
   * iterator throws `ConcurrentModificationException` on a best-effort basis.
   * Why: the headers are a JavaScript array, as Python's are a Python list.
   */
  [Symbol.iterator](): Iterator<Header> {
    return this.#headers[Symbol.iterator]();
  }

  /**
   * Whether `o` is headers of this class holding equal headers in the same
   * order, Java's `RecordHeaders.equals`: `Objects.equals` on the header
   * lists, as `List.equals` compares them. The read-only state is not
   * compared.
   *
   * @param options - the object to compare with
   * @returns true when `o` is equal headers
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
   * @returns true when `o` is equal headers
   */
  equals(o: unknown): boolean;
  equals(options: unknown): boolean {
    EQUALS_FORMS.match(options, arguments.length);
    const o = EQUALS_FORMS.value(options, 'o');
    if (this === o) return true;
    if (!sameClass(this, o)) return false;
    const mine = this.#headers;
    const theirs = (o as Headers).#headers;
    if (mine.length !== theirs.length) return false;
    for (let i = 0; i < mine.length; i++) if (!objectsEquals(mine[i], theirs[i])) return false;
    return true;
  }

  /**
   * The headers' hash code, Java's `RecordHeaders.hashCode`: their list's,
   * `31 * h + header.hashCode()` over the headers, from 1.
   *
   * @returns the hash code, a 32-bit integer
   * @throws `TypeError` when it is given an argument
   */
  hashCode(): number {
    HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    for (const header of this.#headers) h = (Math.imul(31, h) + objectHashCode(header)) | 0;
    return h;
  }

  /**
   * The headers as Java's `RecordHeaders.toString()` writes them:
   * `RecordHeaders(headers = [<each header's text>], isReadOnly = <false, or
   * true once its record was sent>)`.
   *
   * @returns the headers' text
   */
  toString(): string {
    // eslint-disable-next-line @typescript-eslint/no-base-to-string -- Java's List.toString(): each header's own toString(), which a Header the app implements may leave as JavaScript's, as Java's Object.toString()
    const headers = this.#headers.map((h) => String(h)).join(', ');
    return `RecordHeaders(headers = [${headers}], isReadOnly = ${this.#isReadOnly})`;
  }

  /**
   * The headers as `JSON.stringify` writes them: their array, each header
   * written by its own `toJSON()`. It ignores the key `JSON.stringify` passes.
   *
   * @returns the headers, in the order they were added in
   *
   * @remarks *(js-only)* JavaScript's JSON protocol: without it the headers,
   * whose list is a private field, would be written as `{}`.
   */
  toJSON(): Header[] {
    return [...this.#headers];
  }

  /**
   * Java's `RecordHeaders.setReadOnly()`: `KafkaProducer.send` makes the
   * record's headers read-only once the serializers have run, so a later
   * `add` or `remove` throws `IllegalStateError`.
   *
   * @internal
   */
  setReadOnly(): void {
    this.#isReadOnly = true;
  }

  /**
   * Java's `RecordHeaders.isReadOnly()`.
   *
   * @returns whether the headers are read-only
   *
   * @internal
   */
  isReadOnly(): boolean {
    IS_READ_ONLY_FORMS.match(undefined, arguments.length);
    return this.#isReadOnly;
  }

  /**
   * The headers as the native send copies them into the core's
   * `RecordHeaders`: each header's key, as encodeLikeJava's bytes, then its
   * value, read through Java's `key()` and `value()`, as Java's `send()`
   * reads them once they are read-only; null when there are none.
   *
   * @returns the keys' bytes and the values, in order, or null
   * @throws `IllegalArgumentError` when a header's key is null, Java's
   *   `DefaultRecord` error, which the core cannot report: it takes no null
   *   key
   * @throws `TypeError` when a header's key is not a string
   *
   * @internal
   */
  internalToNative(): (Uint8Array | null)[] | null {
    const list = this.#headers;
    if (list.length === 0) return null;
    const out = new Array<Uint8Array | null>(list.length * 2);
    for (let i = 0; i < list.length; i++) {
      const header = list[i];
      const key: unknown = header.key();
      // Java's DefaultRecord refuses it when send() sizes the record.
      if (key === null) throw illegalArgument('Invalid null header key found in headers');
      if (typeof key !== 'string') throw new TypeError('a header key must be a string');
      out[2 * i] = encodeLikeJava(key);
      out[2 * i + 1] = header.value();
    }
    return out;
  }
}
