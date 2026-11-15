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
// one concrete class, the internal RecordHeaders: mutable until its record is
// sent, read-only after, as Java's KafkaProducer.doSend seals the record's
// headers (`setReadOnly`).
//
// It wraps the FFI's RecordHeaders (root CLAUDE.md, JavaScript Binding
// Conventions, Serialization, Q31): every method goes to the addon
// (native/headers.c), so the core holds the headers and refuses a change once
// they are read-only, with Java's text. Option 1 (owner's decision,
// design/current/javascript-producer/DECISIONS.md, entry 21): a Header read
// from headers that can still change is the binding's own copy, which stays
// valid after the headers change; one read from read-only headers is a view,
// with no copy, which keeps them alive.
//
// Headers that never held a header have no native RecordHeaders of their own:
// they read one shared empty RecordHeaders, which nothing changes, until the
// first add makes their own, and sealing them gives them one shared empty
// read-only RecordHeaders, which the core keeps unchangeable. So a record sent
// with no headers makes no native object for them.
//
// Where headers are taken (this constructor, `ProducerRecord`'s `headers`),
// each element is a `Header`, as Java takes them; the core copies them.

import { JavaForms } from '../../internal/args';
import { encodeLikeJava } from '../../internal/encode';
import { objectHashCode, objectsEquals, sameClass } from '../../internal/java_object';
// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('../../internal/native');
import { Header } from './header';

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
 * Whether `value` is a key, a string, or Java's null.
 *
 * @param value - the value to test
 * @returns whether it is a header key
 */
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
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/** The shared empty RecordHeaders that headers with none of their own read: nothing changes it. */
let emptyWritable: object | null = null;
/** The shared empty read-only RecordHeaders of headers sealed before their first add. */
let emptySealed: object | null = null;

/**
 * The shared empty RecordHeaders, made on first use.
 *
 * @returns its native handle
 */
function writableEmpty(): object {
  if (emptyWritable === null) emptyWritable = native.headersNew(null, 0);
  return emptyWritable;
}

/**
 * The shared empty read-only RecordHeaders, made on first use.
 *
 * @returns its native handle
 */
function sealedEmpty(): object {
  if (emptySealed === null) {
    const sealed = native.headersNew(null, 0);
    native.headersSetReadOnly(sealed);
    emptySealed = sealed;
  }
  return emptySealed;
}

/**
 * A key as it crosses to the core: encodeLikeJava's bytes (Node-API would
 * write U+FFFD for a lone surrogate), or null, which the core refuses with
 * Java's IllegalArgumentException "key cannot be null.".
 *
 * @param key - the key given
 * @returns its bytes, or null
 */
function keyBytes(key: unknown): Uint8Array | null {
  if (key === null) return null;
  if (typeof key !== 'string') throw new TypeError('key must be a string');
  return encodeLikeJava(key);
}

/**
 * One taken element's native header, or null for a null element, which the
 * core refuses as Java's `Objects.requireNonNull(header, "Header cannot be
 * null.")` does.
 *
 * @param item - an element of the iterable given
 * @returns the element's native header, or null
 */
function nativeHeaderOf(item: unknown): object | null {
  if (item instanceof Header) return Header.internalNativeOf(item);
  if (item === null) return null;
  if (item === undefined) throw new TypeError('a header must not be undefined. Pass a Header, or leave it out');
  throw new TypeError('a header must be a Header');
}

/**
 * The headers the addon handed out, as `Header` objects.
 *
 * @param handles - the native headers
 * @returns them as `Header` objects, in order
 */
function wrapAll(handles: object[]): Header[] {
  const out = new Array<Header>(handles.length);
  for (let i = 0; i < handles.length; i++) out[i] = Header.internalOf(handles[i]);
  return out;
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
 *   has been closed.". *(js-only)* The headers are the core's native
 *   `RecordHeaders`. A `Header` read from headers that can still change is a
 *   copy, which stays as it is when they change, as Java's does; one read
 *   from read-only headers is a view, with no copy, which keeps them alive.
 */
export class Headers implements Iterable<Header> {
  // The native RecordHeaders (native/headers.c), whose finalizer drops it;
  // null until the first add (the shared empty one is read), or the shared
  // empty read-only one once sealed before it.
  #native: object | null = null;

  /**
   * Creates empty headers, Java's `RecordHeaders()`.
   */
  constructor();
  /**
   * Creates headers holding the given headers in their order, Java's
   * `RecordHeaders(Header[])`; null is no headers.
   *
   * @param options - the headers
   * @throws `TypeError` when a header is null ("Header cannot be null.") or
   *   not a `Header`
   *
   * @remarks *(js-only)* The core copies each header: the `Header` objects
   * given stay the app's own, where Java's keeps them.
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
    if (headers !== undefined) this.#native = Headers.#taken(headers);
  }

  /**
   * The native RecordHeaders holding what `headers` holds, taken as Java's
   * RecordHeaders(Header[]) and RecordHeaders(Iterable<Header>) take them, or
   * null for none.
   *
   * @param headers - the headers given, or null
   * @returns their native RecordHeaders, or null
   */
  static #taken(headers: unknown): object | null {
    if (headers === null) return null;
    if (headers instanceof Headers) {
      const source = headers.#native;
      return source === null || source === emptySealed ? null : native.headersCopy(source);
    }
    if (typeof headers !== 'object' || typeof (headers as { [Symbol.iterator]?: unknown })[Symbol.iterator] !== 'function') {
      throw new TypeError('headers must be a Headers, or an iterable of Header');
    }
    // Java checks the headers one by one, so the first null is the last read:
    // the core reports it, and nothing after it is read.
    const handles: (object | null)[] = [];
    if (Array.isArray(headers)) {
      for (let i = 0; i < headers.length; i++) {
        const handle = nativeHeaderOf(headers[i]);
        handles.push(handle);
        if (handle === null) break;
      }
    } else {
      for (const item of headers as Iterable<unknown>) {
        const handle = nativeHeaderOf(item);
        handles.push(handle);
        if (handle === null) break;
      }
    }
    if (handles.length === 0) return null;
    return native.headersNew(handles, Array.isArray(headers) ? 1 : 2);
  }

  /**
   * New, writable headers holding what `headers` holds, as Java's
   * `ProducerRecord` takes them (`new RecordHeaders(headers)`): null is none,
   * a `Headers` is copied. Throws the constructor's errors.
   *
   * @param headers - the headers given to a record
   * @returns a `Headers` holding them
   *
   * @internal
   */
  static internalTaken(headers: unknown): Headers {
    const out = new Headers();
    out.#native = Headers.#taken(headers);
    return out;
  }

  // The RecordHeaders to read.
  #read(): object {
    return this.#native ?? writableEmpty();
  }

  // The RecordHeaders a change goes to: their own, made on the first add,
  // while they can still change; once they are read-only, the shared empty
  // read-only RecordHeaders, which refuses the change with the core's error.
  // The FFI drops the header views a handle handed out on every mutating
  // call, refused or not, so a change is never sent to read-only headers
  // whose views a Header may hold (javascript/CLAUDE.md, boundary rule 1).
  #write(): object {
    const own = this.#native;
    if (own === null) return (this.#native = native.headersNew(null, 0));
    return native.headersIsReadOnly(own) ? sealedEmpty() : own;
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
   *
   * @remarks *(js-only)* The headers hold a copy of the header: the `Header`
   * given stays the app's own.
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
   * @throws `TypeError` when the key is null, not a string or holds a NUL
   *   ("a header key must be a string without null bytes"), or the value is
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
    if (ADD_FORMS.match(options, arguments.length) === 0) {
      const header = ADD_FORMS.value(options, 'header');
      if (header !== null && !(header instanceof Header)) throw new TypeError('header must be a Header');
      // A null header is the core's error, Java's "Header cannot be null.".
      native.headersAdd(this.#write(), header === null ? null : Header.internalNativeOf(header));
    } else {
      // Java's add(key, value) is add(new RecordHeader(key, value)): the
      // header's checks come first, a null key the core's error whatever the
      // value.
      const key = keyBytes(ADD_FORMS.value(options, 'key'));
      const value = key === null ? null : Header.internalValueBytes(ADD_FORMS.value(options, 'value'));
      native.headersAddKeyValue(this.#write(), key, value);
    }
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
   *   null."), read-only headers included, or the given parameters are not
   *   `{key}`
   * @throws `TypeError` when the key is not a string, holds a NUL ("a header
   *   key must be a string without null bytes") or is `undefined`
   *
   * @remarks *(deviation)* On read-only headers a null key is
   * `IllegalArgumentError`, where Java's `remove` checks the read-only state
   * first. Why: the FFI checks a null argument first, and the binding repeats
   * no precondition of Java's.
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
    const key = keyBytes(REMOVE_FORMS.value(options, 'key'));
    // Headers with none of their own remove nothing from the shared empty
    // ones; only a null key is an error there.
    native.headersRemove(this.#native === null ? writableEmpty() : this.#write(), key);
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
   * @throws `TypeError` when the key is not a string, holds a NUL ("a header
   *   key must be a string without null bytes") or is `undefined`
   *
   * @remarks *(js-only)* While the headers can still change (before their
   * record is sent), the header is the binding's own copy of the key and
   * value, which stays as it is when the headers change, as Java's does.
   * Once they are read-only, it is a view of the header inside them, with no
   * copy, which keeps them alive.
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
    const handle = native.headersLastHeader(this.#read(), keyBytes(LAST_HEADER_FORMS.value(options, 'key')));
    return handle === null ? null : Header.internalOf(handle);
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
   * @throws `TypeError` when the key is not a string or is `undefined`, or,
   *   when iterated, holds a NUL ("a header key must be a string without null
   *   bytes")
   *
   * @remarks *(js-only)* Each header is a copy while the headers can still
   * change, and a view once they are read-only, as `lastHeader` says.
   * *(deviation)* Each iterator reads the headers when it is made, so a change
   * during an iteration is not seen and throws nothing, where Java's throws
   * `ConcurrentModificationException`. Why: the FFI hands the headers out as a
   * list, and Java's `ArrayList` iterators fail fast only on a best-effort
   * basis.
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
    const key = keyBytes(HEADERS_FORMS.value(options, 'key'));
    // Java's checkKey(key) runs at the call: a null key is the core's error now.
    if (key === null) native.headersHeaders(this.#read(), null);
    // eslint-disable-next-line @typescript-eslint/no-this-alias -- the iterable reads the headers when it is iterated, as Java's headers(key) iterates the live list
    const self = this;
    return {
      [Symbol.iterator](): Iterator<Header> {
        return wrapAll(native.headersHeaders(self.#read(), key))[Symbol.iterator]();
      },
    };
  }

  /**
   * Returns all headers as an array, in the order they were added in.
   *
   * @returns the headers as an array, mutating this array will not affect the
   *   Headers, if NO headers are present an empty array is returned.
   * @throws `TypeError` when it is given an argument: it only reads
   *
   * @remarks *(js-only)* Each header is a copy while the headers can still
   * change, and a view once they are read-only, as `lastHeader` says.
   */
  toArray(): Header[] {
    // Java's toArray() takes nothing: an argument is refused, never dropped.
    TO_ARRAY_FORMS.match(undefined, arguments.length);
    return this.#native === null ? [] : wrapAll(native.headersToArray(this.#native));
  }

  /**
   * The headers, in the order they were added in.
   *
   * @returns an iterator over the headers
   *
   * @remarks *(js-only)* Each header is a copy while the headers can still
   * change, and a view once they are read-only, as `lastHeader` says.
   * *(deviation)* The iterator reads the headers when it is made, so a change
   * during an iteration is not seen and throws nothing, where Java's throws
   * `ConcurrentModificationException`. Why: the FFI hands the headers out as a
   * list, and Java's `ArrayList` iterators fail fast only on a best-effort
   * basis.
   */
  [Symbol.iterator](): Iterator<Header> {
    const headers = this.#native === null ? [] : wrapAll(native.headersIterator(this.#native));
    return headers[Symbol.iterator]();
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
    const mine = this.#list();
    const theirs = (o as Headers).#list();
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
    for (const header of this.#list()) h = (Math.imul(31, h) + objectHashCode(header)) | 0;
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
    const readOnly = this.#native !== null && native.headersIsReadOnly(this.#native);
    return `RecordHeaders(headers = [${this.#list()
      .map((h) => h.toString())
      .join(', ')}], isReadOnly = ${readOnly})`;
  }

  /**
   * The headers as `JSON.stringify` writes them: their array, each header
   * written by its own `toJSON()`. It ignores the key `JSON.stringify` passes.
   *
   * @returns the headers, in the order they were added in
   *
   * @remarks *(js-only)* JavaScript's JSON protocol: without it the headers,
   * a native handle in a private field, would be written as `{}`.
   */
  toJSON(): Header[] {
    return this.#list();
  }

  // The headers, as toArray() reads them.
  #list(): Header[] {
    return this.#native === null ? [] : wrapAll(native.headersToArray(this.#native));
  }

  /**
   * Java's `RecordHeaders.setReadOnly()`: `KafkaProducer.send` seals the
   * record's headers once the serializers have run, before the hand-over.
   *
   * @internal
   */
  internalSetReadOnly(): void {
    if (this.#native === null) this.#native = sealedEmpty();
    else native.headersSetReadOnly(this.#native);
  }

  /**
   * The native RecordHeaders the native send copies into the record, or null
   * when the headers have none of their own (no header was ever added).
   *
   * @returns the native headers, or null
   *
   * @internal
   */
  internalNative(): object | null {
    const own = this.#native;
    return own === emptySealed ? null : own;
  }
}
