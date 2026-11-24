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

// RecordHeader: Java's org.apache.kafka.common.header.internals.RecordHeader,
// a plain class holding its key and value. Java has no RecordHeaderTest;
// RecordHeadersTest's assertions on a single header are here too. Not
// translated: RecordHeadersTest's testRecordHeaderIsReadThreadSafe and
// testRecordHeaderWithNullValueIsReadThreadSafe, which read a RecordHeader
// built from ByteBuffers on several threads: there is neither the ByteBuffer
// constructor nor a second thread here.

const assert = require('node:assert/strict');

const header = require('@confluentinc/kafka-javascript/common/header');

const { RecordHeader } = header;

test("a header keeps Java's methods, key() and value(), and the very bytes it is given", () => {
  const value = Buffer.from('v');
  const h = new RecordHeader({ key: 'k', value });
  assert.equal(h.key(), 'k');
  // Java's RecordHeader keeps the array it is given: the same Buffer, on every call.
  assert.equal(h.value(), value);
  assert.equal(h.value(), h.value());
  // So a later write to those bytes changes the header's value, as in Java.
  value[0] = 0x77;
  assert.deepEqual(h.value(), Buffer.from('w'));
  // Private state: no own fields, and nothing to reassign.
  assert.deepEqual(Object.keys(h), []);
  assert.equal(new RecordHeader({ key: 'k', value: null }).value(), null);
  // A Uint8Array is kept, and given back as a Buffer over the same memory.
  const backing = new Uint8Array([1, 2, 3]);
  const u8 = backing.subarray(1);
  const out = new RecordHeader({ key: '', value: u8 }).value();
  assert.ok(Buffer.isBuffer(out));
  assert.deepEqual([...out], [2, 3]);
  assert.equal(out.buffer, backing.buffer);
  assert.equal(out.byteOffset, 1);
  u8[0] = 9;
  assert.equal(out[0], 9);
  // An empty value is an empty Buffer, not null.
  assert.deepEqual(new RecordHeader({ key: 'e', value: new Uint8Array(0) }).value(), Buffer.alloc(0));
  // Readers take no argument.
  // @ts-expect-error an argument to a reader: the read-only check throws TypeError
  assert.throws(() => h.key(5), { name: 'TypeError', message: 'key() only reads: it takes no argument' });
  // @ts-expect-error an argument to a reader: the read-only check throws TypeError
  assert.throws(() => h.value(undefined), { name: 'TypeError', message: 'value() only reads: it takes no argument' });
  // toJSON writes the key and the value.
  assert.equal(JSON.stringify(new RecordHeader({ key: 'k', value: null })), '{"key":"k","value":null}');
  assert.equal(JSON.stringify(new RecordHeader({ key: 'k', value: Buffer.from([1]) })), '{"key":"k","value":{"type":"Buffer","data":[1]}}');
});

test('Header is an interface: only RecordHeader is a class at run time', () => {
  assert.equal(/** @type {Record<string, unknown>} */ (/** @type {unknown} */ (header)).Header, undefined);
  assert.deepEqual(Object.keys(header).sort(), ['Headers', 'RecordHeader']);
});

test('the key is kept as given, as Java keeps the String: a lone surrogate and a NUL stay', () => {
  // They cross to the core only at send(), as encodeLikeJava's bytes, where a
  // NUL is refused (the producer's tests).
  assert.equal(new RecordHeader({ key: 'a\uD800b', value: null }).key(), 'a\uD800b');
  assert.equal(new RecordHeader({ key: 'a\0b', value: null }).key(), 'a\0b');
  assert.equal(new RecordHeader({ key: '\u{1F600}é', value: null }).key(), '\u{1F600}é');
});

test("the constructor keeps Java's message for a null key; a header value is bytes", () => {
  // @ts-expect-error a null key: the constructor throws TypeError with Java's message
  assert.throws(() => new RecordHeader({ key: null, value: null }), { name: 'TypeError', message: 'Null header keys are not permitted' });
  // Java checks the key first: a null key whatever the value.
  // @ts-expect-error a null key and a string value: the constructor throws TypeError with Java's message
  assert.throws(() => new RecordHeader({ key: null, value: 'v' }), { name: 'TypeError', message: 'Null header keys are not permitted' });
  // @ts-expect-error a number key: the type check throws TypeError
  assert.throws(() => new RecordHeader({ key: 1, value: null }), { name: 'TypeError', message: 'key must be a string' });
  // @ts-expect-error a string value: the type check throws TypeError
  assert.throws(() => new RecordHeader({ key: 'k', value: 'v' }), {
    name: 'TypeError',
    message:
      "value must be a Buffer, a Uint8Array or null, not a string: a header value is bytes, so encode it first (Buffer.from(value, 'utf8'))",
  });
  // @ts-expect-error a number value: the type check throws TypeError
  assert.throws(() => new RecordHeader({ key: 'k', value: 5 }), {
    name: 'TypeError',
    message: 'value must be a Buffer, a Uint8Array or null',
  });
  // @ts-expect-error a Uint16Array value: the type check throws TypeError
  assert.throws(() => new RecordHeader({ key: 'k', value: new Uint16Array(1) }), {
    name: 'TypeError',
    message: 'value must be a Buffer, a Uint8Array or null',
  });
  // @ts-expect-error an undefined value: the undefined check throws TypeError
  assert.throws(() => new RecordHeader({ key: 'k', value: undefined }), {
    name: 'TypeError',
    message: "value must not be undefined. Pass null for Java's null",
  });
  // @ts-expect-error a missing value: the form check throws IllegalArgumentError
  assert.throws(() => new RecordHeader({ key: 'k' }), {
    name: 'IllegalArgumentError',
    message: 'RecordHeader() takes one of (key, value); got (key)',
  });
  // @ts-expect-error a bare string: the form check throws IllegalArgumentError
  assert.throws(() => new RecordHeader('k'), {
    name: 'IllegalArgumentError',
    message: 'RecordHeader() takes one of (key, value); got a string',
  });
});

test("toString is RecordHeader's", () => {
  assert.equal(
    String(new RecordHeader({ key: 'k', value: Buffer.from([0, 127, 128, 255]) })),
    'RecordHeader(key = k, value = [0, 127, -128, -1])',
  );
  assert.equal(String(new RecordHeader({ key: 'k', value: null })), 'RecordHeader(key = k, value = null)');
  assert.equal(String(new RecordHeader({ key: 'k', value: new Uint8Array(0) })), 'RecordHeader(key = k, value = [])');
});

// The hash codes Java's own RecordHeader.hashCode() gives for these headers
// (kafka-clients, run on the same keys and values): the key's
// String.hashCode() over UTF-16 code units, then Arrays.hashCode of the
// signed value bytes.
test("equals and hashCode are RecordHeader's", () => {
  const h1 = new RecordHeader({ key: 'k', value: Buffer.from([1, 255, 127, 128]) });
  assert.equal(h1.hashCode(), 959477);
  assert.equal(new RecordHeader({ key: '\u{1F600}é', value: null }).hashCode(), 1703763162);
  assert.equal(new RecordHeader({ key: '', value: new Uint8Array(0) }).hashCode(), 1);
  // Arrays.equals: the same bytes in another array are equal, as is a null value to a null value.
  const same = new RecordHeader({ key: 'k', value: new Uint8Array([1, 255, 127, 128]) });
  assert.ok(h1.equals(same));
  assert.ok(h1.equals({ o: same }));
  assert.equal(h1.hashCode(), same.hashCode());
  assert.ok(new RecordHeader({ key: 'k', value: null }).equals(new RecordHeader({ key: 'k', value: null })));
  assert.ok(!h1.equals(new RecordHeader({ key: 'k', value: null })));
  assert.ok(!h1.equals(new RecordHeader({ key: 'j', value: Buffer.from([1, 255, 127, 128]) })));
  assert.ok(!h1.equals(new RecordHeader({ key: 'k', value: Buffer.from([1, 255, 127]) })));
  /** A subclass of RecordHeader: getClass() != o.getClass(), so its header is another class's. */
  class Sub extends RecordHeader {}
  assert.ok(!h1.equals(new Sub({ key: 'k', value: Buffer.from([1, 255, 127, 128]) })));
  // Another Header implementation, with the same key and value, is another class too.
  const other = { key: () => 'k', value: () => Buffer.from([1, 255, 127, 128]) };
  for (const o of [null, 'k', { key: 'k' }, other]) assert.equal(h1.equals({ o }), false);
  // @ts-expect-error an argument to a reader: the read-only check throws TypeError
  assert.throws(() => h1.hashCode(1), { name: 'TypeError', message: 'hashCode() only reads: it takes no argument' });
});

test('a subclass overriding key() or value() is read through them, as Java reads a header through its methods', () => {
  /** A header whose value is computed. */
  class Computed extends RecordHeader {
    /** @override */
    value() {
      return Buffer.from('computed');
    }
  }
  const h = new Computed({ key: 'c', value: null });
  assert.equal(String(h), 'RecordHeader(key = c, value = [99, 111, 109, 112, 117, 116, 101, 100])');
  assert.ok(h.equals(new Computed({ key: 'c', value: Buffer.from('other') })));
});
