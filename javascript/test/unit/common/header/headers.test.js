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

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError, IllegalStateError } = require('@confluentinc/kafka-javascript');
const { Header, Headers } = require('@confluentinc/kafka-javascript/common/header');

const bytes = (s) => Buffer.from(s, 'utf8');
const header = (key, value) => new Header({ key, value: bytes(value) });

function getCount(headers) {
  return headers.toArray().length;
}

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
  headers.internalSetReadOnly();
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
  headers.internalSetReadOnly();
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
  assert.throws(() => recordHeaders.add(null), { name: 'TypeError', message: 'Header cannot be null.' });
  assert.throws(() => recordHeaders.add({ header: null }), { name: 'TypeError', message: 'Header cannot be null.' });
});

test('shouldThrowNpeWhenAddingCollectionWithNullHeader', () => {
  assert.throws(() => new Headers(new Array(1).fill(null)), { name: 'TypeError', message: 'Header cannot be null.' });
  assert.throws(() => new Headers({ headers: [null] }), { name: 'TypeError', message: 'Header cannot be null.' });
});

// The rest: the forms, the null-key checks and toString.

test("the constructor takes Java's three forms, as {headers} or bare; null is no headers", () => {
  const h = header('a', '1');
  assert.deepEqual(new Headers().toArray(), []);
  assert.deepEqual(new Headers(null).toArray(), []);
  assert.deepEqual(new Headers({ headers: null }).toArray(), []);
  assert.equal(new Headers({ headers: [h] }).toArray()[0], h);
  assert.equal(new Headers(new Set([h])).toArray()[0], h);
  // undefined is never "not given".
  assert.throws(() => new Headers(undefined), {
    name: 'TypeError',
    message: "headers must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  // A copy of a read-only Headers is writable.
  const sealed = new Headers([h]);
  sealed.internalSetReadOnly();
  new Headers({ headers: sealed }).add(h);
  for (const [args, message] of [
    [[undefined], 'a header must not be undefined. Pass a Header, or leave it out'],
    [[{ key: 'k', value: null }], 'a header must be a Header'],
    // Java takes only Header objects: a [key, value] pair is no header.
    [[['k', null]], 'a header must be a Header'],
  ]) {
    assert.throws(() => new Headers(args), { name: 'TypeError', message }, String(args));
  }
  assert.throws(() => new Headers({ headers: 'x' }), {
    name: 'TypeError',
    message: 'headers must be a Headers, or an iterable of Header',
  });
  // A bare value that is no iterable fits no form.
  for (const [bare, kind] of [
    ['x', 'a string'],
    [5, 'a number'],
  ]) {
    assert.throws(() => new Headers(bare), {
      name: 'IllegalArgumentError',
      message: `Headers() takes one of (), (headers); got ${kind}`,
    });
  }
  assert.throws(() => new Headers({ headers: [], other: 1 }), {
    name: 'IllegalArgumentError',
    message: 'Headers() takes one of (), (headers); got (headers, other)',
  });
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
  assert.deepEqual(headers.toArray().map((x) => x.key()), ['a', 'a', 'b']);
  assert.throws(() => headers.add({ key: null, value: null }), { name: 'TypeError', message: 'Null header keys are not permitted' });
  assert.throws(() => headers.add({ key: 'k', value: 'v' }), { name: 'TypeError' });
  assert.throws(() => headers.add({ header: { key: 'k', value: null } }), { name: 'TypeError', message: 'header must be a Header' });
  // A bare value that is no Header fits no form.
  assert.throws(() => headers.add('k'), {
    name: 'IllegalArgumentError',
    message: 'add() takes one of (header), (key, value); got a string',
  });
  assert.throws(() => headers.add({ key: 'k' }), {
    name: 'IllegalArgumentError',
    message: 'add() takes one of (header), (key, value); got (key)',
  });
  assert.throws(() => headers.add(h, h), {
    name: 'IllegalArgumentError',
    message: 'add() takes one of (header), (key, value); got 2 arguments, not one object of named parameters',
  });
  // Java's add(key, value) builds the header before the read-only check.
  headers.internalSetReadOnly();
  assert.throws(() => headers.add({ key: null, value: null }), { name: 'TypeError', message: 'Null header keys are not permitted' });
});

test("a null key is Java's IllegalArgumentException \"key cannot be null.\"", () => {
  const headers = new Headers([header('a', '1')]);
  for (const call of [() => headers.remove(null), () => headers.lastHeader(null), () => headers.headers(null), () => headers.remove({ key: null })]) {
    assert.throws(call, (e) => {
      assert.ok(e instanceof IllegalArgumentError);
      assert.equal(e.message, 'key cannot be null.');
      return true;
    });
  }
  assert.throws(() => headers.lastHeader({ key: 1 }), { name: 'TypeError', message: 'key must be a string' });
  assert.throws(() => headers.lastHeader(1), {
    name: 'IllegalArgumentError',
    message: 'lastHeader() takes one of (key); got a number',
  });
  assert.throws(() => headers.headers(undefined), {
    name: 'TypeError',
    message: "key must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  // remove checks the read-only state first, as Java's canWrite() comes first.
  headers.internalSetReadOnly();
  assert.throws(() => headers.remove(null), { name: 'IllegalStateError', message: 'RecordHeaders has been closed.' });
});

test('headers(key) reads the headers when iterated; toArray is a copy', () => {
  const headers = new Headers([header('a', '1')]);
  const byKey = headers.headers('a');
  headers.add(header('a', '2'));
  assert.deepEqual([...byKey].map((h) => h.value().toString()), ['1', '2']);
  assert.deepEqual([...headers.headers('none')], []);
  const array = headers.toArray();
  array.pop();
  assert.equal(getCount(headers), 2);
  assert.throws(() => headers.toArray(1), { name: 'TypeError', message: 'toArray() only reads: it takes no argument' });
});

test("toString is RecordHeaders' and RecordHeader's; JSON writes the array", () => {
  const headers = new Headers([new Header({ key: 'k', value: new Uint8Array([1, 255]) }), new Header({ key: 'n', value: null })]);
  assert.equal(String(headers), 'RecordHeaders(headers = [RecordHeader(key = k, value = [1, -1]), RecordHeader(key = n, value = null)], isReadOnly = false)');
  headers.internalSetReadOnly();
  assert.equal(String(new Headers()), 'RecordHeaders(headers = [], isReadOnly = false)');
  assert.match(String(headers), /isReadOnly = true\)$/);
  assert.equal(JSON.stringify(new Headers([new Header({ key: 'n', value: null })])), '[{"key":"n","value":null}]');
});
