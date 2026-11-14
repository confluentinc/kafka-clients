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

// Header: Java's Header with the constructor of RecordHeader (Java has no
// RecordHeaderTest; RecordHeadersTest's assertions on a single header are
// here too).

const test = require('node:test');
const assert = require('node:assert/strict');

const { Header } = require('@confluentinc/kafka-javascript/common/header');

test("a header keeps Java's methods: key() and value(), the value a Buffer over the given bytes", () => {
  const value = Buffer.from('v');
  const h = new Header({ key: 'k', value });
  assert.equal(h.key(), 'k');
  assert.equal(h.value(), value);
  // Private state: no own fields, and nothing to reassign.
  assert.deepEqual(Object.keys(h), []);
  assert.equal(new Header({ key: 'k', value: null }).value(), null);
  // A Uint8Array is taken, and returned as a Buffer over the same memory.
  const u8 = new Uint8Array([1, 2, 3]).subarray(1);
  const out = new Header({ key: '', value: u8 }).value();
  assert.ok(Buffer.isBuffer(out));
  assert.deepEqual([...out], [2, 3]);
  u8[0] = 9;
  assert.equal(out[0], 9);
  // Readers take no argument.
  for (const read of [() => h.key(5), () => h.value(undefined)]) {
    assert.throws(read, { name: 'TypeError' });
  }
  assert.throws(() => h.key(5), { name: 'TypeError', message: 'key() only reads: it takes no argument' });
  // toJSON writes the key and the value.
  assert.equal(JSON.stringify(new Header({ key: 'k', value: null })), '{"key":"k","value":null}');
});

test("the constructor keeps Java's message for a null key; a header value is bytes", () => {
  assert.throws(() => new Header({ key: null, value: null }), { name: 'TypeError', message: 'Null header keys are not permitted' });
  assert.throws(() => new Header({ key: 1, value: null }), { name: 'TypeError', message: 'key must be a string' });
  assert.throws(() => new Header({ key: 'k', value: 'v' }), {
    name: 'TypeError',
    message: "value must be a Buffer, a Uint8Array or null, not a string: a header value is bytes, so encode it first (Buffer.from(value, 'utf8'))",
  });
  assert.throws(() => new Header({ key: 'k', value: 5 }), { name: 'TypeError', message: 'value must be a Buffer, a Uint8Array or null' });
  assert.throws(() => new Header({ key: 'k', value: new Uint16Array(1) }), { name: 'TypeError', message: 'value must be a Buffer, a Uint8Array or null' });
  assert.throws(() => new Header({ key: 'k', value: undefined }), { name: 'TypeError', message: "value must not be undefined. Pass null for Java's null" });
  assert.throws(() => new Header({ key: 'k' }), { name: 'IllegalArgumentError', message: 'Header() takes one of (key, value); got (key)' });
  assert.throws(() => new Header('k'), { name: 'IllegalArgumentError', message: 'Header() takes one of (key, value); got a string' });
});

test("toString is RecordHeader's", () => {
  assert.equal(String(new Header({ key: 'k', value: Buffer.from([0, 127, 128, 255]) })), 'RecordHeader(key = k, value = [0, 127, -128, -1])');
  assert.equal(String(new Header({ key: 'k', value: null })), 'RecordHeader(key = k, value = null)');
  assert.equal(String(new Header({ key: 'k', value: new Uint8Array(0) })), 'RecordHeader(key = k, value = [])');
});

// The hash codes Java's own RecordHeader.hashCode() gives for these headers
// (kafka-clients, run on the same keys and values): the key's
// String.hashCode() over UTF-16 code units, then Arrays.hashCode of the
// signed value bytes.
test("equals and hashCode are RecordHeader's", () => {
  const h1 = new Header({ key: 'k', value: Buffer.from([1, 255, 127, 128]) });
  assert.equal(h1.hashCode(), 959477);
  assert.equal(new Header({ key: '\u{1F600}é', value: null }).hashCode(), 1703763162);
  assert.equal(new Header({ key: '', value: new Uint8Array(0) }).hashCode(), 1);
  // Arrays.equals: the same bytes in another array are equal, as is a null value to a null value.
  const same = new Header({ key: 'k', value: new Uint8Array([1, 255, 127, 128]) });
  assert.ok(h1.equals(same));
  assert.ok(h1.equals({ o: same }));
  assert.equal(h1.hashCode(), same.hashCode());
  assert.ok(new Header({ key: 'k', value: null }).equals(new Header({ key: 'k', value: null })));
  assert.ok(!h1.equals(new Header({ key: 'k', value: null })));
  assert.ok(!h1.equals(new Header({ key: 'j', value: Buffer.from([1, 255, 127, 128]) })));
  assert.ok(!h1.equals(new Header({ key: 'k', value: Buffer.from([1, 255, 127]) })));
  // getClass() != o.getClass(): a subclass's header is another class.
  class Sub extends Header {}
  assert.ok(!h1.equals(new Sub({ key: 'k', value: Buffer.from([1, 255, 127, 128]) })));
  for (const o of [null, 'k', { key: 'k' }]) assert.equal(h1.equals({ o }), false);
  assert.throws(() => h1.hashCode(1), { name: 'TypeError', message: 'hashCode() only reads: it takes no argument' });
});
