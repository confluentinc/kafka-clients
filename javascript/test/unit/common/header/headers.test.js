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
  // The core copies each header: equal, not the same object.
  for (const taken of [new Headers({ headers: [h] }), new Headers(new Set([h]))]) {
    const [first] = taken.toArray();
    assert.ok(first.equals(h));
    assert.notEqual(first, h);
  }
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
  assert.deepEqual(
    headers.toArray().map((x) => x.key()),
    ['a', 'a', 'b'],
  );
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

test('a null key is Java\'s IllegalArgumentException "key cannot be null."', () => {
  const headers = new Headers([header('a', '1')]);
  for (const call of [
    () => headers.remove(null),
    () => headers.lastHeader(null),
    () => headers.headers(null),
    () => headers.remove({ key: null }),
  ]) {
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
  assert.deepEqual(
    [...byKey].map((h) => h.value().toString()),
    ['1', '2'],
  );
  assert.deepEqual([...headers.headers('none')], []);
  const array = headers.toArray();
  array.pop();
  assert.equal(getCount(headers), 2);
  assert.throws(() => headers.toArray(1), { name: 'TypeError', message: 'toArray() only reads: it takes no argument' });
});

test("toString is RecordHeaders' and RecordHeader's; JSON writes the array", () => {
  const headers = new Headers([new Header({ key: 'k', value: new Uint8Array([1, 255]) }), new Header({ key: 'n', value: null })]);
  assert.equal(
    String(headers),
    'RecordHeaders(headers = [RecordHeader(key = k, value = [1, -1]), RecordHeader(key = n, value = null)], isReadOnly = false)',
  );
  headers.internalSetReadOnly();
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
  same.internalSetReadOnly();
  assert.throws(() => same.add(h1), { name: 'IllegalStateError' });
  assert.ok(headers.equals({ o: same }));
  // The order counts.
  assert.ok(!headers.equals(new Headers([h2, h1])));
  assert.ok(!headers.equals(new Headers([h1])));
  assert.ok(new Headers().equals(new Headers([])));
  for (const o of [null, [h1, h2], 'headers']) assert.equal(headers.equals({ o }), false);
  assert.throws(() => headers.equals(), { name: 'IllegalArgumentError', message: 'equals() takes one of (o); got ()' });
});

// Option 1 (design/current/javascript-producer/DECISIONS.md, entry 21): the
// headers are the core's native RecordHeaders. A Header read from headers
// that can still change is the binding's own copy; one read from read-only
// headers is a view of the native header, with no copy. The addon's counts
// (headerStats(), test-only) show which.

const native = require('../../../../dist/internal/native');

/** How the addon's counts moved while `fn` ran. */
function counted(fn) {
  const before = native.headerStats();
  const result = fn();
  const after = native.headerStats();
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
  headers.internalSetReadOnly();
  const { result: read, delta } = counted(() => ({
    last: headers.lastHeader('trace'),
    array: headers.toArray(),
    iterated: [...headers],
    byKey: [...headers.headers('span')],
  }));
  assert.equal(delta.headerCopies, 0);
  assert.equal(delta.headerViews, 6);
  // The value is a Buffer over the native header's bytes, not a copy of them.
  const { result: value, delta: valueDelta } = counted(() => read.last.value());
  assert.deepEqual(value, bytes('t1'));
  assert.equal(valueDelta.valueExternal, 1);
  assert.equal(valueDelta.valueCopied, 0);
  assertHeader('trace', 't1', read.array[0]);
  assert.equal(read.iterated[1].key(), 'span');
  assert.equal(read.byKey[0].value(), null);
  // A view reads what a copy reads: equal headers.
  const copy = new Headers(headers).lastHeader('trace');
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
