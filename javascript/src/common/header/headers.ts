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

// Java's Headers, with the constructors, toString() and read-only state of its
// one concrete class, the internal RecordHeaders (as the Python client's
// Headers): mutable until its record is sent, read-only after, as Java's
// KafkaProducer.doSend seals the record's headers (`setReadOnly`).
//
// Where headers are taken (this constructor, `ProducerRecord`'s `headers`),
// each element is a `Header`, as Java takes them.

import { JavaForms } from '../../internal/args';
import { illegalArgument, illegalState } from '../../internal/errors';
import { encodeLikeJava } from '../../internal/encode';
import { Header } from './header';

/** Whether `value` is an object with a callable `[Symbol.iterator]` (an array, a `Headers`, a `Set`, ...). */
function isIterableObject(value: unknown): boolean {
  return (
    (typeof value === 'object' || typeof value === 'function') &&
    value !== null &&
    typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === 'function'
  );
}

/** Whether `value` is a key, a string, or Java's null. */
function isKey(value: unknown): boolean {
  return value === null || typeof value === 'string';
}

// Java's forms, each one-parameter form's bare value picked by its type.
const CONSTRUCTOR_FORMS = new JavaForms('Headers', [[], ['headers']], {
  bareTypes: { headers: (value) => value === null || isIterableObject(value) },
});
const ADD_FORMS = new JavaForms('add', [['header'], ['key', 'value']], {
  undefinedMessages: { value: "value must not be undefined. Pass null for Java's null" },
  bareTypes: { header: (value) => value === null || value instanceof Header },
});
const REMOVE_FORMS = new JavaForms('remove', [['key']], { bareTypes: { key: isKey } });
const LAST_HEADER_FORMS = new JavaForms('lastHeader', [['key']], { bareTypes: { key: isKey } });
const HEADERS_FORMS = new JavaForms('headers', [['key']], { bareTypes: { key: isKey } });
const TO_ARRAY_FORMS = new JavaForms('toArray', [[]], { reader: true });

// RecordHeaders.canWrite's message.
const CLOSED = 'RecordHeaders has been closed.';

/** The headers of a `Headers` that has none, shared until the first `add`. */
const EMPTY: readonly Header[] = Object.freeze([]);

/** A header as the native send takes it: key and value bytes. */
type NativeHeader = [Uint8Array, Uint8Array | null];

/** RecordHeaders.checkKey, plus the type a JavaScript caller can get wrong. */
function checkKey(key: unknown): string {
  if (key === null) throw illegalArgument('key cannot be null.');
  if (typeof key !== 'string') throw new TypeError('key must be a string');
  return key;
}

/**
 * One taken element: a `Header` as it is (Java's RecordHeaders keeps the given
 * object). A null element is Java's
 * `Objects.requireNonNull(header, "Header cannot be null.")`.
 */
function headerOf(item: unknown): Header {
  if (item instanceof Header) return item;
  if (item === null) throw new TypeError('Header cannot be null.');
  if (item === undefined) throw new TypeError("a header must not be undefined. Pass a Header, or leave it out");
  throw new TypeError('a header must be a Header');
}

/** The headers `headers` holds, taken as Java's RecordHeaders(Iterable) takes them. */
function taken(headers: unknown): readonly Header[] {
  if (headers === null) return EMPTY;
  if (headers instanceof Headers) return headers.internalHeaders().length === 0 ? EMPTY : headers.internalHeaders().slice();
  if (
    typeof headers !== 'object' ||
    typeof (headers as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function'
  ) {
    throw new TypeError('headers must be a Headers, or an iterable of Header');
  }
  const out: Header[] = [];
  if (Array.isArray(headers)) {
    for (let i = 0; i < headers.length; i++) out.push(headerOf(headers[i]));
  } else {
    for (const item of headers as Iterable<unknown>) out.push(headerOf(item));
  }
  return out.length === 0 ? EMPTY : out;
}

/**
 * A mutable ordered collection of {@link Header} objects. Note that multiple
 * headers may have the same {@link Header.key | key}.
 *
 * The order of headers is preserved in the order they were added.
 *
 * @remarks Java's `Headers` is an interface whose one class, `RecordHeaders`,
 *   is internal; this class is both, with `RecordHeaders`' constructors and
 *   `toString()`. The headers of a record become read-only when the record is
 *   sent: `add` and `remove` then throw `IllegalStateError` "RecordHeaders
 *   has been closed.".
 */
export class Headers implements Iterable<Header> {
  // A shared empty array until the first add; then the class's own array.
  #headers: readonly Header[];
  // RecordHeaders' isReadOnly.
  #readOnly = false;

  /**
   * Creates empty headers, Java's `RecordHeaders()`.
   */
  constructor();
  /**
   * Creates headers holding the given headers in their order, Java's
   * `RecordHeaders(Header[])`. The `Header` objects are kept, not copied;
   * null is no headers.
   *
   * @param options - the headers
   * @throws `TypeError` when a header is null ("Header cannot be null.") or
   *   not a `Header`
   */
  constructor(options: {
    /** The headers, an array of `Header`, or null. */
    headers: readonly Header[] | null;
  });
  /**
   * Creates headers holding the headers of an iterable in their order, Java's
   * `RecordHeaders(Iterable<Header>)`: a `Headers` is copied, its read-only
   * state not.
   *
   * @param options - the headers
   * @throws `TypeError` when `headers` is not iterable, or an element is not
   *   a `Header`
   */
  constructor(options: {
    /** The headers, a `Headers` or an iterable of `Header`, or null. */
    headers: Iterable<Header> | null;
  });
  /**
   * Creates headers holding the given headers in their order: the headers on
   * their own, the same as `{ headers }`, Java's `RecordHeaders(Header[])`.
   *
   * @param headers - The headers: an array of `Header`, or null
   */
  constructor(headers: readonly Header[] | null);
  /**
   * Creates headers holding the headers of an iterable in their order: the
   * headers on their own, the same as `{ headers }`, Java's
   * `RecordHeaders(Iterable<Header>)`.
   *
   * @param headers - The headers: a `Headers` or an iterable of `Header`, or null
   */
  constructor(headers: Iterable<Header> | null);
  constructor(options?: unknown) {
    const headers = CONSTRUCTOR_FORMS.match(options, arguments.length) === 0 ? undefined : CONSTRUCTOR_FORMS.value(options, 'headers');
    this.#headers = headers === undefined ? EMPTY : taken(headers);
  }

  /**
   * New, writable headers holding what `headers` holds, as Java's
   * `ProducerRecord` takes them (`new RecordHeaders(headers)`): null is none,
   * a `Headers` is copied. Throws the constructor's errors.
   *
   * @internal
   */
  static internalTaken(headers: unknown): Headers {
    const out = new Headers();
    if (headers !== null) out.#headers = taken(headers);
    return out;
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
   * Adds a header (key inside), to the end: the header on its own, the same
   * as `add({header})`.
   *
   * @param header - the Header to be added.
   * @returns this instance of the Headers, once the header is added.
   */
  add(header: Header): Headers;
  add(options: unknown): Headers {
    let header: unknown;
    if (ADD_FORMS.match(options, arguments.length) === 0) {
      header = ADD_FORMS.value(options, 'header');
      if (header === null) throw new TypeError('Header cannot be null.');
      if (!(header instanceof Header)) throw new TypeError('header must be a Header');
    } else {
      // Java's add(key, value) is add(new RecordHeader(key, value)): the
      // header's checks come first.
      header = new Header({ key: ADD_FORMS.value(options, 'key') as string, value: ADD_FORMS.value(options, 'value') as Uint8Array | null });
    }
    this.#canWrite();
    if (this.#headers === EMPTY) this.#headers = [header as Header];
    else (this.#headers as Header[]).push(header as Header);
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
    this.#canWrite();
    const key = checkKey(REMOVE_FORMS.value(options, 'key'));
    if (this.#headers.some((h) => Header.internalKeyOf(h) === key)) {
      const kept = this.#headers.filter((h) => Header.internalKeyOf(h) !== key);
      this.#headers = kept.length === 0 ? EMPTY : kept;
    }
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
    const key = checkKey(LAST_HEADER_FORMS.value(options, 'key'));
    const headers = this.#headers;
    for (let i = headers.length - 1; i >= 0; i--) if (Header.internalKeyOf(headers[i]) === key) return headers[i];
    return null;
  }

  /**
   * Returns all headers for the given key, in the order they were added in,
   * if present.
   *
   * @param options - the key
   * @returns all headers for the given key, in the order they were added in,
   *   if NO headers are present an empty iterable is returned. Each iteration
   *   reads the headers as they are then.
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
    const key = checkKey(HEADERS_FORMS.value(options, 'key'));
    // eslint-disable-next-line @typescript-eslint/no-this-alias -- the iterable reads the headers when it is iterated, as Java's headers(key) iterates the live list
    const self = this;
    return {
      *[Symbol.iterator](): Iterator<Header> {
        for (const header of self.#headers) if (Header.internalKeyOf(header) === key) yield header;
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
    // Java's toArray() takes nothing: an argument is refused, never dropped.
    TO_ARRAY_FORMS.match(undefined, arguments.length);
    return this.#headers.slice();
  }

  /**
   * The headers, in the order they were added in.
   *
   * @returns an iterator over the headers
   */
  [Symbol.iterator](): Iterator<Header> {
    return this.#headers[Symbol.iterator]();
  }

  /**
   * The headers as Java's `RecordHeaders.toString()` writes them:
   * `RecordHeaders(headers = [<each header's text>], isReadOnly = <false, or
   * true once its record was sent>)`.
   *
   * @returns the headers' text
   */
  toString(): string {
    return `RecordHeaders(headers = [${this.#headers.map((h) => h.toString()).join(', ')}], isReadOnly = ${this.#readOnly})`;
  }

  /**
   * The headers as `JSON.stringify` writes them: their array, each header
   * written by its own `toJSON()`. It ignores the key `JSON.stringify` passes.
   *
   * @returns the headers, in the order they were added in
   *
   * @remarks *(js-only)* JavaScript's JSON protocol: without it the headers,
   * held in a private field, would be written as `{}`.
   */
  toJSON(): Header[] {
    return this.#headers.slice();
  }

  /**
   * Java's `RecordHeaders.setReadOnly()`: `KafkaProducer.send` seals the
   * record's headers, after the checks and before the hand-over.
   *
   * @internal
   */
  internalSetReadOnly(): void {
    this.#readOnly = true;
  }

  /**
   * The headers themselves, not a copy: read, never changed, by the caller.
   *
   * @internal
   */
  internalHeaders(): readonly Header[] {
    return this.#headers;
  }

  /**
   * The `[key, value]` pairs the native send takes, or null for none: each
   * key's bytes as Java encodes it (`encodeLikeJava`: Node-API would write
   * U+FFFD for a lone surrogate).
   *
   * @internal
   */
  internalToNative(): NativeHeader[] | null {
    const headers = this.#headers;
    if (headers.length === 0) return null;
    const out: NativeHeader[] = new Array<NativeHeader>(headers.length);
    for (let i = 0; i < headers.length; i++) {
      const header = headers[i];
      out[i] = [encodeLikeJava(Header.internalKeyOf(header)), Header.internalValueOf(header)];
    }
    return out;
  }

  // RecordHeaders.canWrite.
  #canWrite(): void {
    if (this.#readOnly) throw illegalState(CLOSED);
  }
}
