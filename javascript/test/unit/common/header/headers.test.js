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

// Java's RecordHeadersTest (org.apache.kafka.common.header.internals), for
// Headers, which is both Java's Headers interface and its RecordHeaders class.
// Not translated: testHeadersIteratorRemove and the iterator-remove part of
// testReadOnly (a JavaScript iterator has no remove()), and
// testRecordHeaderIsReadThreadSafe / testRecordHeaderWithNullValueIsReadThreadSafe
// (they read a RecordHeader built from ByteBuffers on several threads: there is
// neither the ByteBuffer constructor nor a second thread here).

const assert = require('node:assert/strict');

const { IllegalArgumentError, IllegalStateError } = require('@confluentinc/kafka-javascript');
const { Headers, RecordHeader } = require('@confluentinc/kafka-javascript/common/header');
const { internalHeaders } = require('../../../_internal');

/** @typedef {import('@confluentinc/kafka-javascript/common/header').Header} Header */

/** @param {string} s */
const bytes = (s) => Buffer.from(s, 'utf8');
/**
 * @param {string} key
 * @param {string} value
 */
const header = (key, value) => new RecordHeader({ key, value: bytes(value) });

/** @param {Headers} headers */
function getCount(headers) {
  return headers.toArray().length;
}

/**
 * @param {string} key
 * @param {string} value
 * @param {Header | null | undefined} actual
 */
function assertHeader(key, value, actual) {
  assert.ok(actual);
  assert.equal(actual.key(), key);
  assert.deepEqual(actual.value(), bytes(value));
}

/** @param {unknown} e */
const closed = (e) => {
  assert.ok(e instanceof IllegalStateError);
  assert.equal(e.message, 'RecordHeaders has been closed.');
  return true;
};

test('testAdd', () => {
  const headers = new Headers();
  headers.add(header('key', 'value'));
  assertHeader('key', 'value', headers[Symbol.iterator]().next().value);
  headers.add(header('key2', 'value2'));
  assertHeader('key2', 'value2', headers.lastHeader('key2'));
  assert.equal(getCount(headers), 2);
});

test('testAddHeadersPreserveOrder', () => {
  const headers = new Headers();
  headers.add(header('key', 'value'));
  headers.add(header('key2', 'value2'));
  headers.add(header('key3', 'value3'));
  const headersArr = headers.toArray();
  assertHeader('key', 'value', headersArr[0]);
  assertHeader('key2', 'value2', headersArr[1]);
  assertHeader('key3', 'value3', headersArr[2]);
  assert.equal(getCount(headers), 3);
});

test('testRemove', () => {
  const headers = new Headers();
  headers.add(header('key', 'value'));
  assert.equal(headers[Symbol.iterator]().next().done, false);
  headers.remove('key');
  assert.equal(headers[Symbol.iterator]().next().done, true);
});

test('testPreserveOrderAfterRemove', () => {
  const headers = new Headers();
  headers.add(header('key', 'value'));
  headers.add(header('key2', 'value2'));
  headers.add(header('key3', 'value3'));
  headers.remove('key');
  let headersArr = headers.toArray();
  assertHeader('key2', 'value2', headersArr[0]);
  assertHeader('key3', 'value3', headersArr[1]);
  assert.equal(getCount(headers), 2);
  headers.add(header('key4', 'value4'));
  headers.remove('key3');
  headersArr = headers.toArray();
  assertHeader('key2', 'value2', headersArr[0]);
  assertHeader('key4', 'value4', headersArr[1]);
  assert.equal(getCount(headers), 2);
});

test('testAddRemoveInterleaved', () => {
  const headers = new Headers();
  headers.add(header('key', 'value'));
  headers.add(header('key2', 'value2'));
  assert.equal(headers[Symbol.iterator]().next().done, false);
  headers.remove('key');
  assert.equal(getCount(headers), 1);
  headers.add(header('key3', 'value3'));
  assert.equal(headers.lastHeader('key'), null);
  assertHeader('key2', 'value2', headers.lastHeader('key2'));
  assertHeader('key3', 'value3', headers.lastHeader('key3'));
  assert.equal(getCount(headers), 2);
  headers.remove('key2');
  assert.equal(headers.lastHeader('key'), null);
  assert.equal(headers.lastHeader('key2'), null);
  assertHeader('key3', 'value3', headers.lastHeader('key3'));
  assert.equal(getCount(headers), 1);
  headers.add(header('key3', 'value4'));
  assertHeader('key3', 'value4', headers.lastHeader('key3'));
  assert.equal(getCount(headers), 2);
  headers.add(header('key', 'valueNew'));
  assert.equal(getCount(headers), 3);
  assertHeader('key', 'valueNew', headers.lastHeader('key'));
  headers.remove('key3');
  assert.equal(getCount(headers), 1);
  assert.equal(headers.lastHeader('key2'), null);
  headers.remove('key');
  assert.equal(headers[Symbol.iterator]().next().done, true);
});

test('testLastHeader', () => {
  const headers = new Headers();
  headers.add(header('key', 'value'));
  headers.add(header('key', 'value2'));
  headers.add(header('key', 'value3'));
  assertHeader('key', 'value3', headers.lastHeader('key'));
  assert.equal(getCount(headers), 3);
});

test('testReadOnly', () => {
  const headers = new Headers();
  headers.add(header('key', 'value'));
  const headerIteratorBeforeClose = headers[Symbol.iterator]();
  internalHeaders(headers).setReadOnly();
  assert.throws(() => headers.add(header('key', 'value')), closed);
  assert.throws(() => headers.add({ key: 'key', value: null }), closed);
  assert.throws(() => headers.remove('key'), closed);
  // Reading still works, through an iterator made before or after.
  assertHeader('key', 'value', headerIteratorBeforeClose.next().value);
  assertHeader('key', 'value', headers[Symbol.iterator]().next().value);
});

test('testHeaders', () => {
  const headers = new Headers();
  headers.add(header('key', 'value'));
  headers.add(header('key1', 'key1value'));
  headers.add(header('key', 'value2'));
  headers.add(header('key2', 'key2value'));
  let keyHeaders = headers.headers('key')[Symbol.iterator]();
  assertHeader('key', 'value', keyHeaders.next().value);
  assertHeader('key', 'value2', keyHeaders.next().value);
  assert.equal(keyHeaders.next().done, true);
  keyHeaders = headers.headers('key1')[Symbol.iterator]();
  assertHeader('key1', 'key1value', keyHeaders.next().value);
  assert.equal(keyHeaders.next().done, true);
  keyHeaders = headers.headers('key2')[Symbol.iterator]();
  assertHeader('key2', 'key2value', keyHeaders.next().value);
  assert.equal(keyHeaders.next().done, true);
});

test('testNew', () => {
  const headers = new Headers();
  headers.add(header('key', 'value'));
  internalHeaders(headers).setReadOnly();
  const newHeaders = new Headers(headers);
  newHeaders.add(header('key', 'value2'));
  // Ensure existing headers are not modified
  assertHeader('key', 'value', headers.lastHeader('key'));
  assert.equal(getCount(headers), 1);
  // Ensure new headers are modified
  assertHeader('key', 'value2', newHeaders.lastHeader('key'));
  assert.equal(getCount(newHeaders), 2);
});

test('shouldThrowNpeWhenAddingNullHeader', () => {
  const recordHeaders = new Headers();
  // @ts-expect-error a null header: add throws TypeError with Java's message
  assert.throws(() => recordHeaders.add(null), { name: 'TypeError', message: 'Header cannot be null.' });
  // @ts-expect-error a null header: add throws TypeError with Java's message
  assert.throws(() => recordHeaders.add({ header: null }), { name: 'TypeError', message: 'Header cannot be null.' });
});

test('shouldThrowNpeWhenAddingCollectionWithNullHeader', () => {
  assert.throws(() => new Headers(new Array(1).fill(null)), { name: 'TypeError', message: 'Header cannot be null.' });
  // @ts-expect-error a null among the headers: the constructor throws TypeError with Java's message
  assert.throws(() => new Headers({ headers: [null] }), { name: 'TypeError', message: 'Header cannot be null.' });
  // Java checks the headers one by one, so nothing after the first null is read.
  function* nullThenThrow() {
    yield header('a', '1');
    yield null;
    throw new Error('read past the null');
  }
  // @ts-expect-error a generator yielding null: the constructor throws TypeError with Java's message
  assert.throws(() => new Headers(nullThenThrow()), { name: 'TypeError', message: 'Header cannot be null.' });
  // @ts-expect-error a null and a string among the headers: the constructor throws TypeError with Java's message
  assert.throws(() => new Headers([header('a', '1'), null, 'not a header']), { name: 'TypeError', message: 'Header cannot be null.' });
});

// The rest: the forms, the [key, value] pairs, the very objects held, the
// null-key checks and toString.

test("the constructor takes Java's three forms, as {headers} or bare; null is no headers", () => {
  const h = header('a', '1');
  assert.deepEqual(new Headers().toArray(), []);
  assert.deepEqual(new Headers(null).toArray(), []);
  assert.deepEqual(new Headers({ headers: null }).toArray(), []);
  // undefined is never "not given".
  // @ts-expect-error undefined for the headers: the undefined check throws TypeError
  assert.throws(() => new Headers(undefined), {
    name: 'TypeError',
    message: "headers must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  // @ts-expect-error a string for the headers: the type check throws TypeError
  assert.throws(() => new Headers({ headers: 'x' }), {
    name: 'TypeError',
    message: 'headers must be a Headers, or an iterable of Header objects and [key, value] pairs',
  });
  // A bare value that is no iterable fits no form.
  for (const [bare, kind] of [
    ['x', 'a string'],
    [5, 'a number'],
  ]) {
    // @ts-expect-error a bare string or number: the form check throws IllegalArgumentError
    assert.throws(() => new Headers(bare), {
      name: 'IllegalArgumentError',
      message: `Headers() takes one of (), (headers); got ${kind}`,
    });
  }
  // @ts-expect-error an unknown key: the form check throws IllegalArgumentError
  assert.throws(() => new Headers({ headers: [], other: 1 }), {
    name: 'IllegalArgumentError',
    message: 'Headers() takes one of (), (headers); got (headers, other)',
  });
  // @ts-expect-error undefined headers: the undefined check throws TypeError
  assert.throws(() => new Headers({ headers: undefined }), {
    name: 'TypeError',
    message: "headers must not be undefined. Leave the key out, or pass null for Java's null",
  });
  // Each form holds what it is given.
  for (const taken of [
    new Headers([h]),
    new Headers({ headers: [h] }),
    new Headers(new Set([h])),
    new Headers({ headers: new Set([h]) }),
  ]) {
    assert.equal(taken.toArray()[0], h);
  }
});

test('the headers hold the very Header objects given, as Java does; a copy of a Headers holds them too, writable', () => {
  const a = header('a', '1');
  const b = header('b', '2');
  const headers = new Headers([a]);
  assert.equal(headers.add(b), headers);
  const [first, second] = headers.toArray();
  assert.equal(first, a);
  assert.equal(second, b);
  assert.equal(headers.lastHeader('b'), b);
  assert.equal([...headers.headers('a')][0], a);
  assert.equal([...headers][1], b);
  // The same header may be added twice, and to other headers.
  headers.add(a);
  assert.deepEqual(headers.toArray(), [a, b, a]);
  // A header read stays valid whatever its headers do later, as Java's does.
  headers.remove('a');
  assert.deepEqual(headers.toArray(), [b]);
  assertHeader('a', '1', a);
  // Java's new RecordHeaders(headers): a new list of the same objects, writable, the read-only state not copied.
  internalHeaders(headers).setReadOnly();
  const copy = new Headers(headers);
  assert.equal(copy.toArray()[0], b);
  copy.add(a);
  assert.equal(getCount(headers), 1);
  assert.equal(internalHeaders(copy).isReadOnly(), false);
});

test('a [key, value] pair is a header too, a new RecordHeader, mixed in any order with Header objects', () => {
  const h = header('h', 'x');
  const value = bytes('v');
  /** @type {Array<Header | readonly [string, Uint8Array | null]>} */
  const given = [['a', value], h, ['n', null], ['e', new Uint8Array(0)]];
  for (const headers of [new Headers(given), new Headers({ headers: given }), new Headers(new Set(given)), new Headers(given.values())]) {
    const [a, kept, n, e] = headers.toArray();
    assert.ok(a instanceof RecordHeader);
    assert.equal(a.key(), 'a');
    // The pair's value, kept as a RecordHeader keeps it: no copy.
    assert.equal(a.value(), value);
    assert.equal(kept, h);
    assert.ok(n instanceof RecordHeader && n.value() === null);
    assert.deepEqual(e.value(), Buffer.alloc(0));
    assert.equal(e.value()?.length, 0);
  }
  // A frozen array is a pair as well, and a pair given as Uint8Array keeps its memory.
  const u8 = new Uint8Array([1, 2]);
  const [frozen] = new Headers([Object.freeze(/** @type {const} */ (['f', u8]))]).toArray();
  assert.equal(frozen.value()?.buffer, u8.buffer);
});

test('a [key, value] pair is checked with messages naming its position; a null element and a null key are Java NPEs', () => {
  /** @param {unknown[]} elements */
  const take = (elements) => () => new Headers(/** @type {Iterable<Header>} */ (/** @type {unknown} */ (elements)));
  const h = header('h', 'x');
  for (const [elements, message] of [
    [[h, ['k', 'v']], 'headers[1] value must be a Buffer, a Uint8Array or null; got string'],
    [[h, [1, null]], 'headers[1] key must be a string; got number'],
    [[[undefined, null]], 'headers[0] key must be a string; got undefined'],
    [[h, h, ['k', undefined]], "headers[2] value must not be undefined. Pass null for Java's null"],
    [[['k', null, 'extra']], 'headers[0] must be a Header or a [key, value] pair; got Array(3)'],
    [[['k']], 'headers[0] must be a Header or a [key, value] pair; got Array(1)'],
    [[h, 5], 'headers[1] must be a Header or a [key, value] pair; got number'],
    [[{ key: 'k', value: null }], 'headers[0] must be a Header or a [key, value] pair; got Object'],
    [[new Map([['k', null]])], 'headers[0] must be a Header or a [key, value] pair; got Map'],
    [[h, ['k', new Uint16Array(1)]], 'headers[1] value must be a Buffer, a Uint8Array or null; got Uint16Array'],
    [[h, undefined], 'headers[1] must not be undefined. Pass a Header or a [key, value] pair, or leave it out'],
    // Java's own NullPointerExceptions.
    [[h, null], 'Header cannot be null.'],
    [[[null, bytes('v')]], 'Null header keys are not permitted'],
  ]) {
    assert.throws(take(/** @type {unknown[]} */ (elements)), { name: 'TypeError', message: String(message) }, String(message));
  }
});

test('a Header the app implements is a Header: kept, read through its key() and value()', () => {
  let reads = 0;
  /** @type {Header} */
  const own = {
    key() {
      reads++;
      return 'own';
    },
    value() {
      return bytes('mine');
    },
  };
  const headers = new Headers([own]);
  assert.equal(headers.lastHeader('own'), own);
  assert.ok(reads > 0);
  // add(header) takes it by name: a plain object given bare is the {key, value} options object.
  headers.add({ header: own });
  assert.deepEqual(headers.toArray(), [own, own]);
  // Its equality is identity, as Java's Object.equals.
  assert.ok(headers.equals(new Headers([own, own])));
  assert.ok(!headers.equals(new Headers([own, header('own', 'mine')])));
  // @ts-expect-error a plain object that is no Header: the header check throws TypeError
  assert.throws(() => headers.add({ header: { key: 'k' } }), { name: 'TypeError', message: 'header must be a Header' });
  // Given bare, a plain object is the {key, value} options object, so
  // TypeScript takes only a RecordHeader bare.
  const more = new Headers();
  // @ts-expect-error a plain object given bare: read as {key, value}, whose key is no string, it throws TypeError
  assert.throws(() => more.add(own), { name: 'TypeError', message: 'key must be a string' });
  assert.equal(more.toArray().length, 0);
  // An instance of the app's own Header class is no plain object, so plain
  // JavaScript may give it bare.
  /** A Header class of the app's own. */
  class Own {
    key() {
      return 'class';
    }
    value() {
      return null;
    }
  }
  const instance = new Own();
  // @ts-expect-error a Header of another class goes by name in TypeScript, while plain JavaScript gives it bare
  more.add(instance);
  assert.equal(more.lastHeader('class'), instance);
});

test('add takes {header}, a bare RecordHeader or {key, value}; it returns the headers', () => {
  const headers = new Headers();
  const h = header('a', '1');
  assert.equal(headers.add(h), headers);
  assert.equal(headers.add({ header: h }), headers);
  assert.equal(headers.add({ key: 'b', value: null }), headers);
  const added = headers.toArray();
  assert.deepEqual(
    added.map((x) => x.key()),
    ['a', 'a', 'b'],
  );
  // add(key, value) makes a new RecordHeader, which keeps the value given.
  const value = bytes('v');
  headers.add({ key: 'c', value });
  const c = headers.lastHeader('c');
  assert.ok(c instanceof RecordHeader);
  assert.equal(c.value(), value);
  // @ts-expect-error a null key: the header constructor throws TypeError with Java's message
  assert.throws(() => headers.add({ key: null, value: null }), { name: 'TypeError', message: 'Null header keys are not permitted' });
  // @ts-expect-error a string value: the value check throws TypeError
  assert.throws(() => headers.add({ key: 'k', value: 'v' }), { name: 'TypeError' });
  // @ts-expect-error a plain object for a Header: the header check throws TypeError
  assert.throws(() => headers.add({ header: { key: 'k', value: null } }), { name: 'TypeError', message: 'header must be a Header' });
  // A bare value that is no Header fits no form.
  // @ts-expect-error a bare string: the form check throws IllegalArgumentError
  assert.throws(() => headers.add('k'), {
    name: 'IllegalArgumentError',
    message: 'add() takes one of (header), (key, value); got a string',
  });
  // @ts-expect-error a bare pair: add takes one header, not the pairs of the iterables
  assert.throws(() => headers.add(['k', null]), {
    name: 'IllegalArgumentError',
    message: 'add() takes one of (header), (key, value); got an array',
  });
  // @ts-expect-error a key without a value: the form check throws IllegalArgumentError
  assert.throws(() => headers.add({ key: 'k' }), {
    name: 'IllegalArgumentError',
    message: 'add() takes one of (header), (key, value); got (key)',
  });
  // @ts-expect-error a second argument: the form check throws IllegalArgumentError
  assert.throws(() => headers.add(h, h), {
    name: 'IllegalArgumentError',
    message: 'add() takes one of (header), (key, value); got 2 arguments, not one object of named parameters',
  });
  // Java's add(header) checks for null before the read-only state, and
  // add(key, value) builds the header first.
  internalHeaders(headers).setReadOnly();
  // @ts-expect-error a null header: add throws TypeError with Java's message
  assert.throws(() => headers.add(null), { name: 'TypeError', message: 'Header cannot be null.' });
  // @ts-expect-error a null key: the header constructor throws TypeError before the read-only check
  assert.throws(() => headers.add({ key: null, value: null }), { name: 'TypeError', message: 'Null header keys are not permitted' });
  assert.throws(() => headers.add({ key: 'k', value: null }), closed);
});

test('a null key is Java\'s IllegalArgumentException "key cannot be null."', () => {
  const headers = new Headers([header('a', '1')]);
  for (const call of [
    // @ts-expect-error a null key: remove throws IllegalArgumentError with Java's message
    () => headers.remove(null),
    // @ts-expect-error a null key: lastHeader throws IllegalArgumentError with Java's message
    () => headers.lastHeader(null),
    // @ts-expect-error a null key: headers throws IllegalArgumentError with Java's message
    () => headers.headers(null),
    // @ts-expect-error a null key: remove throws IllegalArgumentError with Java's message
    () => headers.remove({ key: null }),
  ]) {
    assert.throws(call, (e) => {
      assert.ok(e instanceof IllegalArgumentError);
      assert.equal(e.message, 'key cannot be null.');
      return true;
    });
  }
  // @ts-expect-error a number key: the key check throws TypeError
  assert.throws(() => headers.lastHeader({ key: 1 }), { name: 'TypeError', message: 'key must be a string' });
  // @ts-expect-error a bare number: the form check throws IllegalArgumentError
  assert.throws(() => headers.lastHeader(1), {
    name: 'IllegalArgumentError',
    message: 'lastHeader() takes one of (key); got a number',
  });
  // @ts-expect-error undefined for the key: the undefined check throws TypeError
  assert.throws(() => headers.headers(undefined), {
    name: 'TypeError',
    message: "key must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  // Java's remove checks canWrite() before checkKey(): on read-only headers a
  // null key is the read-only error.
  internalHeaders(headers).setReadOnly();
  // @ts-expect-error a null key: on read-only headers remove throws Java's IllegalStateError first
  assert.throws(() => headers.remove(null), closed);
  // @ts-expect-error a number key: the key check throws TypeError, before the read-only check
  assert.throws(() => headers.remove({ key: 1 }), { name: 'TypeError', message: 'key must be a string' });
  assert.throws(() => headers.remove('a'), closed);
  assert.equal(getCount(headers), 1);
  // The reads still check the key.
  // @ts-expect-error a null key: lastHeader throws IllegalArgumentError with Java's message
  assert.throws(() => headers.lastHeader(null), { name: 'IllegalArgumentError', message: 'key cannot be null.' });
});

test('headers(key) reads the headers when iterated; toArray is a copy', () => {
  const headers = new Headers([header('a', '1')]);
  const byKey = headers.headers('a');
  headers.add(header('a', '2'));
  assert.deepEqual(
    [...byKey].map((h) => {
      const value = h.value();
      assert.ok(value);
      return value.toString();
    }),
    ['1', '2'],
  );
  assert.deepEqual([...headers.headers('none')], []);
  const array = headers.toArray();
  array.pop();
  assert.equal(getCount(headers), 2);
  // @ts-expect-error an argument to a reader: the read-only check throws TypeError
  assert.throws(() => headers.toArray(1), { name: 'TypeError', message: 'toArray() only reads: it takes no argument' });
});

test("an iterator sees a change made during the iteration, as a JavaScript array's does", () => {
  const headers = new Headers([header('a', '1')]);
  const iterator = headers[Symbol.iterator]();
  assertHeader('a', '1', iterator.next().value);
  headers.add(header('b', '2'));
  assertHeader('b', '2', iterator.next().value);
  assert.equal(iterator.next().done, true);
});

test('the read-only state: setReadOnly() and isReadOnly(), the library-only members send() uses', () => {
  const headers = new Headers();
  const internal = internalHeaders(headers);
  assert.equal(internal.isReadOnly(), false);
  internal.setReadOnly();
  assert.equal(internal.isReadOnly(), true);
  // Again: still read-only.
  internal.setReadOnly();
  assert.equal(internal.isReadOnly(), true);
  const call = /** @type {Record<string, (...args: unknown[]) => unknown>} */ (/** @type {unknown} */ (internal));
  assert.throws(() => call.isReadOnly(1), { name: 'TypeError', message: 'isReadOnly() only reads: it takes no argument' });
});

test("toString is RecordHeaders' and RecordHeader's; JSON writes the array", () => {
  const headers = new Headers([
    new RecordHeader({ key: 'k', value: new Uint8Array([1, 255]) }),
    new RecordHeader({ key: 'n', value: null }),
  ]);
  assert.equal(
    String(headers),
    'RecordHeaders(headers = [RecordHeader(key = k, value = [1, -1]), RecordHeader(key = n, value = null)], isReadOnly = false)',
  );
  internalHeaders(headers).setReadOnly();
  assert.equal(String(new Headers()), 'RecordHeaders(headers = [], isReadOnly = false)');
  assert.match(String(headers), /isReadOnly = true\)$/);
  assert.equal(JSON.stringify(new Headers([new RecordHeader({ key: 'n', value: null })])), '[{"key":"n","value":null}]');
  // A Header the app implements prints its own toString(), JavaScript's when it has none.
  assert.equal(
    String(new Headers([{ key: () => 'o', value: () => null }])),
    'RecordHeaders(headers = [[object Object]], isReadOnly = false)',
  );
});

// The hash codes Java's own RecordHeaders.hashCode() gives (kafka-clients, run
// on the same headers): the list's, 31 * h + each header's, from 1.
test("equals and hashCode are RecordHeaders'", () => {
  const h1 = new RecordHeader({ key: 'k', value: Buffer.from([1, 255, 127, 128]) });
  const h2 = new RecordHeader({ key: '\u{1F600}é', value: null });
  const headers = new Headers([h1, h2]);
  assert.equal(headers.hashCode(), 1733507910);
  assert.equal(new Headers().hashCode(), 1);
  const same = new Headers([
    new RecordHeader({ key: 'k', value: Buffer.from([1, 255, 127, 128]) }),
    new RecordHeader({ key: '\u{1F600}é', value: null }),
  ]);
  assert.ok(headers.equals(same));
  assert.equal(headers.hashCode(), same.hashCode());
  // The read-only state is not compared.
  internalHeaders(same).setReadOnly();
  assert.throws(() => same.add(h1), { name: 'IllegalStateError' });
  assert.ok(headers.equals({ o: same }));
  // The order counts.
  assert.ok(!headers.equals(new Headers([h2, h1])));
  assert.ok(!headers.equals(new Headers([h1])));
  assert.ok(new Headers().equals(new Headers([])));
  // A pair makes an equal RecordHeader.
  assert.ok(headers.equals(new Headers([['k', Buffer.from([1, 255, 127, 128])], h2])));
  for (const o of [null, [h1, h2], 'headers']) assert.equal(headers.equals({ o }), false);
  // @ts-expect-error no argument: the form check throws IllegalArgumentError
  assert.throws(() => headers.equals(), { name: 'IllegalArgumentError', message: 'equals() takes one of (o); got ()' });
});

test("the native send's copy: each key as encodeLikeJava's bytes, each value the header's own, null for none", () => {
  const value = bytes('v');
  const headers = new Headers([
    ['a\uD800', value],
    ['n', null],
  ]);
  const native = internalHeaders(headers).internalToNative();
  assert.ok(native);
  assert.deepEqual(
    native.map((b) => (b === null ? null : Buffer.from(b).toString('hex'))),
    [Buffer.from('a?').toString('hex'), value.toString('hex'), Buffer.from('n').toString('hex'), null],
  );
  // The value is the header's own Buffer: no copy here (the addon copies it into the core's RecordHeaders).
  assert.equal(native[1], value);
  assert.equal(internalHeaders(new Headers()).internalToNative(), null);
  // A Header the app implements is read through its methods: a null key is
  // Java's IllegalArgumentException from DefaultRecord, which Java's send()
  // throws, and a key that is no string is refused.
  const nullKey = new Headers([{ key: () => /** @type {string} */ (/** @type {unknown} */ (null)), value: () => null }]);
  assert.throws(() => internalHeaders(nullKey).internalToNative(), {
    name: 'IllegalArgumentError',
    message: 'Invalid null header key found in headers',
  });
  const bad = new Headers([{ key: () => /** @type {string} */ (/** @type {unknown} */ (5)), value: () => null }]);
  assert.throws(() => internalHeaders(bad).internalToNative(), { name: 'TypeError', message: 'a header key must be a string' });
});
