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
const { Header, Headers } = require('@confluentinc/kafka-javascript/common/header');
const { internalHeaders } = require('../../../_internal');

/** @param {string} s */
const bytes = (s) => Buffer.from(s, 'utf8');
/**
 * @param {string} key
 * @param {string} value
 */
const header = (key, value) => new Header({ key, value: bytes(value) });

/** @param {Headers} headers */
function getCount(headers) {
  return headers.toArray().length;
}

/**
 * @param {string} key
 * @param {string} value
 * @param {unknown} actual
 */
function assertHeader(key, value, actual) {
  assert.ok(actual instanceof Header);
  assert.equal(actual.key(), key);
  assert.deepEqual(actual.value(), bytes(value));
}

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
  const before = headers[Symbol.iterator]();
  internalHeaders(headers).internalSetReadOnly();
  /** @param {unknown} e */
  const closed = (e) => {
    assert.ok(e instanceof IllegalStateError);
    assert.equal(e.message, 'RecordHeaders has been closed.');
    return true;
  };
  assert.throws(() => headers.add(header('key', 'value')), closed);
  assert.throws(() => headers.add({ key: 'key', value: null }), closed);
  assert.throws(() => headers.remove('key'), closed);
  // Reading still works, through an iterator made before or after.
  assertHeader('key', 'value', before.next().value);
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
  internalHeaders(headers).internalSetReadOnly();
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
  // The core reports the first null, and nothing after it is read, as Java's
  // loop stops there.
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

// The rest: the forms, the null-key checks and toString.

test("the constructor takes Java's three forms, as {headers} or bare; null is no headers", () => {
  const h = header('a', '1');
  assert.deepEqual(new Headers().toArray(), []);
  assert.deepEqual(new Headers(null).toArray(), []);
  assert.deepEqual(new Headers({ headers: null }).toArray(), []);
  // The core copies each header: equal, not the same object.
  for (const taken of [new Headers({ headers: [h] }), new Headers(new Set([h]))]) {
    const [first] = taken.toArray();
    assert.ok(first.equals(h));
    assert.notEqual(first, h);
  }
  // undefined is never "not given".
  // @ts-expect-error undefined for the headers: the undefined check throws TypeError
  assert.throws(() => new Headers(undefined), {
    name: 'TypeError',
    message: "headers must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  // A copy of a read-only Headers is writable.
  const sealed = new Headers([h]);
  internalHeaders(sealed).internalSetReadOnly();
  new Headers({ headers: sealed }).add(h);
  for (const [args, message] of [
    [[undefined], 'a header must not be undefined. Pass a Header, or leave it out'],
    [[{ key: 'k', value: null }], 'a header must be a Header'],
    // Java takes only Header objects: a [key, value] pair is no header.
    [[['k', null]], 'a header must be a Header'],
  ]) {
    // @ts-expect-error values that are no Header: the header check throws TypeError
    assert.throws(() => new Headers(args), { name: 'TypeError', message }, String(args));
  }
  // @ts-expect-error a string for the headers: the type check throws TypeError
  assert.throws(() => new Headers({ headers: 'x' }), {
    name: 'TypeError',
    message: 'headers must be a Headers, or an iterable of Header',
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
});

test('add takes {header}, a bare Header or {key, value}; it returns the headers', () => {
  const headers = new Headers();
  const h = header('a', '1');
  assert.equal(headers.add(h), headers);
  assert.equal(headers.add({ header: h }), headers);
  assert.equal(headers.add({ key: 'b', value: null }), headers);
  assert.deepEqual(
    headers.toArray().map((x) => x.key()),
    ['a', 'a', 'b'],
  );
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
  // Java's add(key, value) builds the header before the read-only check.
  internalHeaders(headers).internalSetReadOnly();
  // @ts-expect-error a null key: the header constructor throws TypeError before the read-only check
  assert.throws(() => headers.add({ key: null, value: null }), { name: 'TypeError', message: 'Null header keys are not permitted' });
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
  // (deviation) Java's remove checks canWrite() before checkKey(): on read-only
  // headers a null key is the FFI's error, which checks the NULL argument first.
  internalHeaders(headers).internalSetReadOnly();
  assert.throws(
    // @ts-expect-error a null key: remove throws IllegalArgumentError with Java's message
    () => headers.remove(null),
    (e) => {
      assert.ok(e instanceof IllegalArgumentError);
      assert.equal(e.message, 'key cannot be null.');
      return true;
    },
  );
  // @ts-expect-error a number key: the key check throws TypeError
  assert.throws(() => headers.remove({ key: 1 }), { name: 'TypeError', message: 'key must be a string' });
  assert.throws(() => headers.remove('a'), { name: 'IllegalStateError', message: 'RecordHeaders has been closed.' });
  assert.equal(getCount(headers), 1);
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

test("toString is RecordHeaders' and RecordHeader's; JSON writes the array", () => {
  const headers = new Headers([new Header({ key: 'k', value: new Uint8Array([1, 255]) }), new Header({ key: 'n', value: null })]);
  assert.equal(
    String(headers),
    'RecordHeaders(headers = [RecordHeader(key = k, value = [1, -1]), RecordHeader(key = n, value = null)], isReadOnly = false)',
  );
  internalHeaders(headers).internalSetReadOnly();
  assert.equal(String(new Headers()), 'RecordHeaders(headers = [], isReadOnly = false)');
  assert.match(String(headers), /isReadOnly = true\)$/);
  assert.equal(JSON.stringify(new Headers([new Header({ key: 'n', value: null })])), '[{"key":"n","value":null}]');
});

// The hash codes Java's own RecordHeaders.hashCode() gives (kafka-clients, run
// on the same headers): the list's, 31 * h + each header's, from 1.
test("equals and hashCode are RecordHeaders'", () => {
  const h1 = new Header({ key: 'k', value: Buffer.from([1, 255, 127, 128]) });
  const h2 = new Header({ key: '\u{1F600}é', value: null });
  const headers = new Headers([h1, h2]);
  assert.equal(headers.hashCode(), 1733507910);
  assert.equal(new Headers().hashCode(), 1);
  const same = new Headers([
    new Header({ key: 'k', value: Buffer.from([1, 255, 127, 128]) }),
    new Header({ key: '\u{1F600}é', value: null }),
  ]);
  assert.ok(headers.equals(same));
  assert.equal(headers.hashCode(), same.hashCode());
  // The read-only state is not compared.
  internalHeaders(same).internalSetReadOnly();
  assert.throws(() => same.add(h1), { name: 'IllegalStateError' });
  assert.ok(headers.equals({ o: same }));
  // The order counts.
  assert.ok(!headers.equals(new Headers([h2, h1])));
  assert.ok(!headers.equals(new Headers([h1])));
  assert.ok(new Headers().equals(new Headers([])));
  for (const o of [null, [h1, h2], 'headers']) assert.equal(headers.equals({ o }), false);
  // @ts-expect-error no argument: the form check throws IllegalArgumentError
  assert.throws(() => headers.equals(), { name: 'IllegalArgumentError', message: 'equals() takes one of (o); got ()' });
});

// Option 1 (design/current/javascript-producer/DECISIONS.md, entry 21): the
// headers are the core's native RecordHeaders. A Header read from headers
// that can still change is the binding's own copy; one read from read-only
// headers is a view of the native header, with no copy. The addon's counts
// (headerStats(), test-only) show which.

const native = require('../../../../dist/internal/native');

/**
 * How the addon's counts moved while `fn` ran.
 *
 * @template T
 * @param {() => T} fn
 * @returns {{ result: T, delta: Record<string, number> }}
 */
function counted(fn) {
  const before = native.headerStats();
  const result = fn();
  const after = native.headerStats();
  /** @type {Record<string, number>} */
  const delta = {};
  for (const name of Object.keys(after)) delta[name] = after[name] - before[name];
  return { result, delta };
}

test('a Header read from headers that can still change is a copy, which keeps its key and value after they change', () => {
  const headers = new Headers();
  headers.add({ key: 'trace', value: bytes('t1') });
  headers.add({ key: 'span', value: bytes('s1') });
  const { result: read, delta } = counted(() => ({
    last: headers.lastHeader('trace'),
    array: headers.toArray(),
    iterated: [...headers],
    byKey: [...headers.headers('span')],
  }));
  // One copy per header handed out (1 + 2 + 2 + 1), and no view.
  assert.equal(delta.headerCopies, 6);
  assert.equal(delta.headerViews, 0);
  assert.ok(read.last);
  const value = read.last.value();
  // The headers change: Java's Header stays as it was, and so does the copy.
  headers.add({ key: 'trace', value: bytes('t2') });
  headers.remove('span');
  for (let i = 0; i < 100; i++) headers.add({ key: `k${i}`, value: Buffer.alloc(64, i) });
  headers.remove('trace');
  assertHeader('trace', 't1', read.last);
  assert.equal(read.last.value(), value);
  assert.deepEqual(value, bytes('t1'));
  assertHeader('trace', 't1', read.array[0]);
  assertHeader('span', 's1', read.array[1]);
  assertHeader('span', 's1', read.iterated[1]);
  assertHeader('span', 's1', read.byKey[0]);
  assert.equal(headers.lastHeader('trace'), null);
});

test('a Header read from read-only headers is a view of the native header, with no copy', () => {
  const headers = new Headers();
  headers.add({ key: 'trace', value: bytes('t1') });
  headers.add({ key: 'span', value: null });
  internalHeaders(headers).internalSetReadOnly();
  const { result: read, delta } = counted(() => ({
    last: headers.lastHeader('trace'),
    array: headers.toArray(),
    iterated: [...headers],
    byKey: [...headers.headers('span')],
  }));
  assert.equal(delta.headerCopies, 0);
  assert.equal(delta.headerViews, 6);
  // The value is a Buffer over the native header's bytes, not a copy of them.
  const { result: value, delta: valueDelta } = counted(() => {
    assert.ok(read.last);
    return read.last.value();
  });
  assert.deepEqual(value, bytes('t1'));
  assert.equal(valueDelta.valueExternal, 1);
  assert.equal(valueDelta.valueCopied, 0);
  assertHeader('trace', 't1', read.array[0]);
  assert.equal(read.iterated[1].key(), 'span');
  assert.equal(read.byKey[0].value(), null);
  // A view reads what a copy reads: equal headers.
  const copy = new Headers(headers).lastHeader('trace');
  assert.ok(copy);
  assert.ok(read.last);
  assert.ok(copy.equals(read.last));
  assert.equal(copy.hashCode(), read.last.hashCode());
});

test("add(header) stores a copy: the app's Header stays its own", () => {
  const mine = header('k', 'v');
  const headers = new Headers();
  headers.add(mine);
  headers.remove('k');
  assertHeader('k', 'v', mine);
  assert.equal(getCount(headers), 0);
  // And the header can go into other headers, or be added twice.
  headers.add(mine).add(mine);
  assert.equal(getCount(headers), 2);
  assert.notEqual(headers.lastHeader('k'), mine);
});

// The lifetimes of the native headers (native/headers.c): a Header view
// keeps its read-only Headers' native list alive, a value Buffer keeps its
// header's native bytes alive, and every handle is destroyed exactly once,
// when the last of them is collected. Run in a child with --expose-gc (and,
// on macOS, MallocScribble, which fills freed memory, so a read of a freed
// header shows), counting through the addon's headerStats().

const { spawnSync } = require('node:child_process');
const path = require('node:path');

const PACKAGE_ROOT = path.join(__dirname, '..', '..', '..', '..');

const PROBE = `
const assert = require('node:assert/strict');
const native = require(${JSON.stringify(path.join(PACKAGE_ROOT, 'dist', 'internal', 'native'))});
const { Header, Headers } = require(${JSON.stringify(path.join(PACKAGE_ROOT, 'dist', 'common', 'header'))});
const { KafkaProducer, ProducerRecord } = require(${JSON.stringify(path.join(PACKAGE_ROOT, 'dist', 'producer'))});

// A few collections, with timer turns between them so the finalizers run
// (an external Buffer's runs from a later task than a wrapped object's).
async function collect() {
  for (let i = 0; i < 10; i++) {
    globalThis.gc();
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

function churn() {
  for (let round = 0; round < 50; round++) {
    const h = new Headers([new Header({ key: 'a', value: Buffer.alloc(32, round) })]);
    h.add({ key: 'b', value: null });
    h.toArray();
    if (round % 2 === 0) h.internalSetReadOnly();
    for (const x of h) x.value();
    [...new Headers(h).headers('a')];
  }
}

async function refusedChangesKeepViews() {
  const record = new ProducerRecord({
    topic: 't',
    partition: null,
    key: null,
    value: Buffer.from('v'),
    headers: [
      new Header({ key: 'trace', value: Buffer.from('abcdefgh') }),
      new Header({ key: 'span', value: Buffer.from('s1') }),
      new Header({ key: 'trace', value: Buffer.from('t2') }),
    ],
  });
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 200 } });
  try {
    // send() seals the record's headers; its delivery fails, which is not what is tested.
    await producer.send(record).catch(() => {});
    const headers = record.headers;
    const read = headers.lastHeader('trace');
    assert.equal(read.key(), 'trace');
    assert.equal(read.value().toString(), 't2');
    const unread = { last: headers.lastHeader('span'), array: headers.toArray(), iterated: [...headers], byKey: [...headers.headers('trace')] };
    const closed = { name: 'IllegalStateError', message: 'RecordHeaders has been closed.' };
    assert.throws(() => headers.add(new Header({ key: 'x', value: Buffer.from('x') })), closed);
    assert.throws(() => headers.add({ key: 'x', value: null }), closed);
    assert.throws(() => headers.remove('span'), closed);
    assert.throws(() => headers.remove(null), { name: 'IllegalArgumentError', message: 'key cannot be null.' });
    // The record sent again: a second seal.
    await producer.send(record).catch(() => {});
    globalThis.gc();
    const expect = (h, key, value) => {
      assert.equal(h.key(), key);
      assert.equal(h.value().toString(), value);
    };
    expect(unread.last, 'span', 's1');
    for (const list of [unread.array, unread.iterated]) {
      expect(list[0], 'trace', 'abcdefgh');
      expect(list[1], 'span', 's1');
      expect(list[2], 'trace', 't2');
    }
    expect(unread.byKey[0], 'trace', 'abcdefgh');
    expect(unread.byKey[1], 'trace', 't2');
    // A view already read, and the others, passed on to other headers.
    const passed = new Headers([read, unread.array[1]]);
    const other = new Headers();
    other.add(read).add(unread.byKey[0]).add({ key: 'k', value: null });
    assert.deepEqual(passed.toArray().map((h) => [h.key(), h.value().toString()]), [['trace', 't2'], ['span', 's1']]);
    assert.deepEqual(
      other.toArray().map((h) => [h.key(), h.value() === null ? null : h.value().toString()]),
      [['trace', 't2'], ['trace', 'abcdefgh'], ['k', null]],
    );
    assert.ok(headers.equals(new Headers(record.headers)));
  } finally {
    await producer.close();
  }
}

(async () => {
  // The module's shared empty headers, made on first use, live for the process.
  new Headers().internalSetReadOnly();
  new Headers().lastHeader('warm');
  await collect();
  const base = native.headerStats();
  const live = () => {
    const s = native.headerStats();
    return {
      headers: (s.headersMade - base.headersMade) - (s.headersFreed - base.headersFreed),
      header: (s.headerMade - base.headerMade) - (s.headerFreed - base.headerFreed),
    };
  };

  // A view keeps its Headers alive, and a value Buffer its header.
  let headers = new Headers();
  headers.add({ key: 'trace', value: Buffer.from('abcdefgh') });
  headers.internalSetReadOnly();
  let view = headers.lastHeader('trace');
  headers = null;
  await collect();
  assert.deepEqual(live(), { headers: 1, header: 1 }, 'the view keeps its Headers');
  assert.equal(view.key(), 'trace');
  let value = view.value();
  view = null;
  await collect();
  assert.deepEqual(live(), { headers: 1, header: 1 }, 'the value keeps its header and its Headers');
  assert.equal(value.toString(), 'abcdefgh');
  value = null;
  await collect();
  assert.deepEqual(live(), { headers: 0, header: 0 }, 'all freed once nothing holds them');

  // A copy holds nothing of its Headers: they go while it stays.
  headers = new Headers();
  headers.add({ key: 'k', value: Buffer.from('copied') });
  let copy = headers.lastHeader('k');
  headers = null;
  await collect();
  assert.deepEqual(live(), { headers: 0, header: 1 }, 'a copy does not keep its Headers');
  assert.equal(copy.value().toString(), 'copied');
  copy = null;

  // The views of read-only headers stay valid through a refused add or remove,
  // as Java's Header does: the FFI drops a handle's views on every mutating
  // call, refused or not, so the binding takes the core's refusal from headers
  // that hand out none. Some views are read before the refused calls, some
  // after, and a second seal (the record sent again) keeps them too.
  await refusedChangesKeepViews();
  await collect();
  assert.deepEqual(live(), { headers: 0, header: 0 }, 'the views and their Headers freed after the refused calls');

  // Many of each, dropped (in a function of their own, whose frame then holds
  // none): every handle destroyed exactly once.
  churn();
  await collect();
  assert.deepEqual(live(), { headers: 0, header: 0 }, 'every handle freed');
  const s = native.headerStats();
  assert.ok(s.headersFreed <= s.headersMade && s.headerFreed <= s.headerMade, 'nothing freed twice');
  console.log('ok');
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
`;

test('a Header keeps its Headers alive, a value Buffer its Header, and each handle is destroyed once', () => {
  const r = spawnSync(process.execPath, ['--expose-gc', '--no-warnings', '-e', PROBE], {
    encoding: 'utf8',
    env: { ...process.env, MallocScribble: '1', MallocGuardEdges: '1' },
    timeout: 60000,
  });
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'ok\n');
});
