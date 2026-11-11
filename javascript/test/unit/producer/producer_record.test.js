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

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { KafkaError } = require('@confluentinc/kafka-javascript/common');
const { Header, Headers } = require('@confluentinc/kafka-javascript/common/header');
const { ProducerRecord } = require('@confluentinc/kafka-javascript/producer');

const bytes = (s) => Buffer.from(s, 'utf8');
const H = (key, value) => new Header({ key, value });
/** The native send's header pairs with their keys as text (they cross as encodeLikeJava's bytes). */
const keysAsText = (pairs) => (pairs === null ? null : pairs.map(([key, value]) => [Buffer.from(key).toString(), value]));

test('the fields Java leaves out default to null and no headers', () => {
  const r = new ProducerRecord({ topic: 't', value: 'v' });
  assert.equal(r.topic, 't');
  assert.equal(r.partition, null);
  assert.equal(r.timestamp, null);
  assert.equal(r.key, null);
  assert.equal(r.value, 'v');
  assert.ok(r.headers instanceof Headers);
  assert.deepEqual(r.headers.toArray(), []);
});

// A data object: Java's getters are own enumerable fields, set once in the
// constructor, so spread, JSON.stringify and structuredClone keep them.
test('the fields are own data properties, in Java order, kept by spread, JSON and structuredClone', () => {
  const headers = [new Header({ key: 'h', value: bytes('x') })];
  const r = new ProducerRecord({ topic: 't', partition: 1, timestamp: 5, key: 'k', value: 'v', headers });
  const names = ['topic', 'partition', 'timestamp', 'key', 'value', 'headers'];
  assert.deepEqual(Object.keys(r), names);
  for (const name of names) {
    const d = Object.getOwnPropertyDescriptor(r, name);
    assert.ok('value' in d && d.enumerable, `${name} is an own enumerable data property`);
  }
  for (const name of names) assert.equal(Object.getOwnPropertyDescriptor(ProducerRecord.prototype, name), undefined);
  const fields = { topic: 't', partition: 1, timestamp: 5, key: 'k', value: 'v' };
  assert.deepEqual({ ...r }, { ...fields, headers: r.headers });
  // JSON writes the headers as their array (Headers' toJSON), each a {key,
  // value} data object; structuredClone keeps the fields, and the headers'
  // own (none: Headers holds its list in a private field).
  assert.deepEqual(JSON.parse(JSON.stringify(r)), { ...fields, headers: [{ key: 'h', value: { type: 'Buffer', data: [120] } }] });
  assert.deepEqual(structuredClone(r), { ...fields, headers: {} });
  // Not frozen (readonly is TypeScript's); the headers are the record's own
  // Headers, holding the given Header objects, as Java's new RecordHeaders(headers).
  assert.equal(Object.isFrozen(r), false);
  assert.equal(r.headers.toArray()[0], headers[0]);
  // A record with no headers has its own, empty and writable, Headers.
  const empty = new ProducerRecord({ topic: 't', value: 'v' });
  assert.notEqual(empty.headers, new ProducerRecord({ topic: 'u', value: null }).headers);
  empty.headers.add({ key: 'a', value: null });
  assert.equal(empty.headers.toArray().length, 1);
});

test('headers are taken as Java takes an Iterable<Header>: a Headers is copied, the Header objects kept', () => {
  const given = new Headers([new Header({ key: 'a', value: bytes('1') })]);
  given.internalSetReadOnly();
  const r = new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: given });
  // A copy, writable, holding the same Header objects.
  assert.notEqual(r.headers, given);
  assert.equal(r.headers.toArray()[0], given.toArray()[0]);
  r.headers.add({ key: 'b', value: null });
  assert.equal(given.toArray().length, 1);
  // An iterable that is not an array, and null.
  const k = new Header({ key: 'k', value: null });
  const l = new Header({ key: 'l', value: bytes('x') });
  const set = new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: new Set([k, l]) });
  assert.deepEqual(set.headers.toArray(), [k, l]);
  // A [key, value] pair is no Header: Java takes only Header objects.
  assert.throws(() => new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: [['k', null]] }), {
    name: 'TypeError',
    message: 'a header must be a Header',
  });
  assert.deepEqual(new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: null }).headers.toArray(), []);
});

const FORMS =
  'ProducerRecord() takes one of (topic, partition, timestamp, key, value, headers), ' +
  '(topic, partition, timestamp, key, value), (topic, partition, key, value, headers), ' +
  '(topic, partition, key, value), (topic, key, value), (topic, value)';

// Java's ProducerRecordTest.testInvalidRecords, with Java's messages.
// (testEqualsAndHashCode is not translated: Java's ProducerRecord overrides
// equals() and hashCode(), which have no JavaScript counterpart, and the
// client's ProducerRecord defines neither.)
test("Java's constructor checks keep Java's messages", () => {
  assert.throws(() => new ProducerRecord({ topic: null, partition: 0, key: 'key', value: 'v' }), (e) => {
    assert.ok(e instanceof IllegalArgumentError);
    assert.ok(!(e instanceof KafkaError));
    assert.equal(e.name, 'IllegalArgumentError');
    assert.ok(e instanceof RangeError);
    assert.equal(e.message, 'Topic cannot be null.');
    return true;
  });
  // An undefined topic is refused: leave the key out, or pass null.
  assert.throws(() => new ProducerRecord({ topic: undefined, partition: 0, key: 'key', value: 'v' }), {
    name: 'TypeError',
    message: "topic must not be undefined. Leave the key out, or pass null for Java's null",
  });
  // A topic left out matches no Java constructor.
  assert.throws(() => new ProducerRecord({ partition: 0, key: 'key', value: 'v' }), {
    name: 'IllegalArgumentError',
    message: `${FORMS}; got (partition, key, value)`,
  });
  assert.throws(() => new ProducerRecord({ topic: 'test', partition: 0, timestamp: -1, key: 'key', value: 'v' }), {
    name: 'IllegalArgumentError',
    message: 'Invalid timestamp: -1. Timestamp should always be non-negative or null.',
  });
  assert.throws(() => new ProducerRecord({ topic: 'test', partition: -1, key: 'key', value: 'v' }), {
    name: 'IllegalArgumentError',
    message: 'Invalid partition: -1. Partition number should always be non-negative or null.',
  });
  assert.throws(() => new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: [new Header({ key: null, value: bytes('x') })] }), {
    name: 'TypeError',
    message: 'Null header keys are not permitted',
  });
  // A null header: Java's RecordHeaders requireNonNull(header, "Header cannot be null.").
  assert.throws(() => new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: [null] }), {
    name: 'TypeError',
    message: 'Header cannot be null.',
  });
});

test("Java's six constructors each build the record they name", () => {
  const h = [new Header({ key: 'h', value: bytes('x') })];
  const cases = [
    [{ topic: 't', partition: 1, timestamp: 2, key: 'k', value: 'v', headers: h }, [1, 2, 'k', 'v', 1]],
    [{ topic: 't', partition: 1, timestamp: 2, key: 'k', value: 'v' }, [1, 2, 'k', 'v', 0]],
    [{ topic: 't', partition: 1, key: 'k', value: 'v', headers: h }, [1, null, 'k', 'v', 1]],
    [{ topic: 't', partition: 1, key: 'k', value: 'v' }, [1, null, 'k', 'v', 0]],
    [{ topic: 't', key: 'k', value: 'v' }, [null, null, 'k', 'v', 0]],
    [{ topic: 't', value: 'v' }, [null, null, null, 'v', 0]],
    // null is Java's null, given: it selects the constructor taking it.
    [{ topic: 't', partition: null, timestamp: null, key: null, value: null, headers: null }, [null, null, null, null, 0]],
  ];
  for (const [options, [partition, timestamp, key, value, headers]] of cases) {
    const r = new ProducerRecord(options);
    assert.equal(r.topic, 't');
    assert.equal(r.partition, partition);
    assert.equal(r.timestamp, timestamp);
    assert.equal(r.key, key);
    assert.equal(r.value, value);
    assert.equal(r.headers.toArray().length, headers);
  }
});

test("a parameter given as undefined is a TypeError; value's message says null sends a tombstone", () => {
  assert.throws(() => new ProducerRecord({ topic: 't', value: undefined }), {
    name: 'TypeError',
    message: "value must not be undefined. Pass null for Java's null, which sends a tombstone.",
  });
  for (const name of ['partition', 'timestamp', 'key', 'headers']) {
    const options = { topic: 't', partition: null, timestamp: null, key: null, value: 'v', headers: null, [name]: undefined };
    assert.throws(() => new ProducerRecord(options), {
      name: 'TypeError',
      message: `${name} must not be undefined. Leave the key out, or pass null for Java's null`,
    });
  }
});

test('undefined inside the headers is a TypeError; a null header value is Java\'s null', () => {
  const record = (headers) => new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers });
  const [header] = record([new Header({ key: 'h', value: null })]).headers.toArray();
  assert.deepEqual([header.key(), header.value()], ['h', null]);
  assert.throws(() => record([undefined]), { name: 'TypeError', message: 'a header must not be undefined. Pass a Header, or leave it out' });
  assert.throws(() => new Header({ key: 'h', value: undefined }), { name: 'TypeError', message: "value must not be undefined. Pass null for Java's null" });
});

test('a call with no argument, undefined or more than one argument is refused', () => {
  assert.throws(() => new ProducerRecord(undefined), {
    name: 'TypeError',
    message: "ProducerRecord() takes one object of named parameters, not undefined: leave the argument out, or pass null for Java's null",
  });
  assert.throws(() => new ProducerRecord({ topic: 't', value: 'v' }, 'extra'), {
    name: 'IllegalArgumentError',
    message: `${FORMS}; got 2 arguments, not one object of named parameters`,
  });
});

test('the constructor reads only the parameters the form check saw: own enumerable keys', () => {
  // An object whose prototype holds parameters is no options object.
  const inherited = Object.assign(Object.create({ partition: 3, key: 'k', timestamp: 7, headers: null }), {
    topic: 't',
    value: 'v',
  });
  assert.throws(() => new ProducerRecord(inherited), {
    name: 'IllegalArgumentError',
    message: `${FORMS}; got an object`,
  });
  // A non-enumerable own partition is not given.
  const hidden = { topic: 't', value: 'v' };
  Object.defineProperty(hidden, 'partition', { value: 3, enumerable: false });
  assert.equal(new ProducerRecord(hidden).partition, null);
  // Nor is a partition on a polluted Object.prototype.
  Object.prototype.partition = 0;
  try {
    assert.equal(new ProducerRecord({ topic: 't', value: 'v' }).partition, null);
  } finally {
    delete Object.prototype.partition;
  }
});

test('every set of parameters that is not a Java constructor is an IllegalArgumentError', () => {
  const names = ['topic', 'partition', 'timestamp', 'key', 'value', 'headers'];
  const values = { topic: 't', partition: 0, timestamp: 0, key: 'k', value: 'v', headers: [] };
  const java = new Set([
    'topic,partition,timestamp,key,value,headers',
    'topic,partition,timestamp,key,value',
    'topic,partition,key,value,headers',
    'topic,partition,key,value',
    'topic,key,value',
    'topic,value',
  ]);
  let rejected = 0;
  for (let mask = 0; mask < 1 << names.length; mask++) {
    const given = names.filter((n, i) => mask & (1 << i));
    const options = Object.fromEntries(given.map((n) => [n, values[n]]));
    if (java.has(given.join(','))) {
      assert.ok(new ProducerRecord(options) instanceof ProducerRecord, given.join(', '));
      continue;
    }
    rejected += 1;
    assert.throws(() => new ProducerRecord(options), (e) => {
      assert.ok(e instanceof IllegalArgumentError);
      assert.equal(e.message, `${FORMS}; got (${given.join(', ')})`);
      return true;
    });
  }
  assert.equal(rejected, 64 - 6);
  // A name no constructor has.
  assert.throws(() => new ProducerRecord({ topic: 't', value: 'v', offset: 0 }), {
    name: 'IllegalArgumentError',
    message: `${FORMS}; got (topic, value, offset)`,
  });
});

test('values of the wrong type are TypeErrors', () => {
  assert.throws(() => new ProducerRecord(), { name: 'IllegalArgumentError', message: `${FORMS}; got ()` });
  assert.throws(() => new ProducerRecord('t'), {
    name: 'IllegalArgumentError',
    message: `${FORMS}; got a string`,
  });
  assert.throws(() => new ProducerRecord({ topic: 1, value: 'v' }), { name: 'TypeError', message: 'topic must be a string' });
  // A key and a value are what the producer's serializers take, as Java's
  // ProducerRecord<K, V> holds any K and V: without a serializer, send()
  // refuses one that is not bytes or a string.
  const order = { id: 1 };
  const typed = new ProducerRecord({ topic: 't', key: 1, value: order });
  assert.equal(typed.key, 1);
  assert.equal(typed.value, order);
  assert.throws(() => typed.internalToNative(), {
    name: 'TypeError',
    message: 'key must be a Buffer, a Uint8Array, a string or null when the producer has no key serializer',
  });
  assert.throws(() => new ProducerRecord({ topic: 't', value: {} }).internalToNative(), {
    name: 'TypeError',
    message: 'value must be a Buffer, a Uint8Array, a string or null when the producer has no value serializer',
  });
  assert.throws(() => new ProducerRecord({ topic: 't', partition: 1.5, key: null, value: 'v' }), {
    name: 'TypeError',
    message: 'partition must be an integer or null',
  });
  assert.throws(() => new ProducerRecord({ topic: 't', partition: 2 ** 31, key: null, value: 'v' }), {
    name: 'RangeError',
    message: 'partition must be at most 2147483647',
  });
  assert.throws(() => new ProducerRecord({ topic: 't', partition: null, timestamp: 2 ** 53, key: null, value: 'v' }), {
    name: 'RangeError',
    message: 'timestamp must be at most 9007199254740991',
  });
  assert.throws(() => new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: 'x' }), {
    name: 'TypeError',
    message: 'headers must be a Headers, or an iterable of Header',
  });
  // A {key, value} object is not a Header.
  assert.throws(() => new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: [{ key: 'h', value: null }] }), {
    name: 'TypeError',
    message: 'a header must be a Header',
  });
});

test("the caller's key and value bytes cross as they are, marked for the addon to copy", () => {
  const key = Buffer.from('k');
  const value = new Uint8Array([1, 2, 3]);
  const r = new ProducerRecord({ topic: 't', partition: 3, timestamp: 1000, key, value });
  const [topic, partition, timestamp, k, v, owned, headers] = r.internalToNative();
  assert.equal(topic, 't');
  assert.equal(partition, 3);
  assert.equal(timestamp, 1000);
  assert.equal(k, key);
  assert.equal(v, value);
  // Neither is the client's own: native/record.c copies both.
  assert.equal(owned, 0);
  assert.equal(headers, null);
});

test('strings are encoded as UTF-8 and null stays null', () => {
  const r = new ProducerRecord({
    topic: 't',
    partition: null,
    key: 'clé',
    value: null,
    headers: [H('h', bytes('é')), H('n', null)],
  });
  const [, partition, timestamp, k, v, owned, headers] = r.internalToNative();
  assert.equal(partition, -1);
  assert.equal(timestamp, -1);
  assert.deepEqual(k, Buffer.from('clé', 'utf8'));
  assert.equal(v, null);
  // The encoded key is the client's own: not copied again.
  assert.equal(owned, 1);
  assert.deepEqual(keysAsText(headers), [['h', bytes('é')], ['n', null]]);
  assert.equal(new ProducerRecord({ topic: 't', key: Buffer.from('k'), value: 'v' }).internalToNative()[5], 2);
});

test('send seals the headers once the fields are checked, as Java\'s doSend calls setReadOnly', () => {
  const r = new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: [H('a', null)] });
  r.headers.add({ key: 'b', value: null });
  r.internalToNative();
  const closed = { name: 'IllegalStateError', message: 'RecordHeaders has been closed.' };
  assert.throws(() => r.headers.add({ key: 'c', value: null }), closed);
  assert.throws(() => r.headers.remove('a'), closed);
  assert.match(String(r), /isReadOnly = true/);
  // Reading is unaffected, and so is a second send of the same record.
  assert.equal(r.headers.lastHeader('b').key(), 'b');
  assert.deepEqual(keysAsText(r.internalToNative()[6]), [['a', null], ['b', null]]);
  // A record whose re-check fails leaves its headers writable.
  const bad = new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: null });
  bad.value = 42;
  assert.throws(() => bad.internalToNative(), { name: 'TypeError' });
  bad.headers.add({ key: 'ok', value: null });
});

// The fields are not frozen: plain JavaScript can reassign one after the
// constructor checked it. The send-time re-check (internalToNative, which
// send() calls) throws what the constructor throws for that value.
const REASSIGNED = [
  ['value', undefined, { name: 'TypeError', message: "value must not be undefined. Pass null for Java's null, which sends a tombstone." }],
  ['key', undefined, { name: 'TypeError', message: "key must not be undefined. Leave the key out, or pass null for Java's null" }],
  ['key', 42, { name: 'TypeError', message: 'key must be a Buffer, a Uint8Array, a string or null when the producer has no key serializer' }],
  ['value', {}, { name: 'TypeError', message: 'value must be a Buffer, a Uint8Array, a string or null when the producer has no value serializer' }],
  ['headers', [['h', null]], { name: 'TypeError', message: 'a header must be a Header' }],
  ['headers', [{ key: 'h', value: null }], { name: 'TypeError', message: 'a header must be a Header' }],
  ['headers', [undefined], { name: 'TypeError', message: 'a header must not be undefined. Pass a Header, or leave it out' }],
  ['headers', [null], { name: 'TypeError', message: 'Header cannot be null.' }],
  ['headers', 'x', { name: 'TypeError', message: 'headers must be a Headers, or an iterable of Header' }],
  ['headers', undefined, { name: 'TypeError', message: "headers must not be undefined. Leave the key out, or pass null for Java's null" }],
  ['timestamp', NaN, { name: 'TypeError', message: 'timestamp must be an integer or null' }],
  ['timestamp', -5, { name: 'IllegalArgumentError', message: 'Invalid timestamp: -5. Timestamp should always be non-negative or null.' }],
  ['timestamp', 2 ** 53, { name: 'RangeError', message: `timestamp must be at most ${Number.MAX_SAFE_INTEGER}` }],
  ['partition', NaN, { name: 'TypeError', message: 'partition must be an integer or null' }],
  ['partition', 0.5, { name: 'TypeError', message: 'partition must be an integer or null' }],
  ['partition', Infinity, { name: 'TypeError', message: 'partition must be an integer or null' }],
  ['partition', 2 ** 32, { name: 'RangeError', message: 'partition must be at most 2147483647' }],
  ['partition', -5, { name: 'IllegalArgumentError', message: 'Invalid partition: -5. Partition number should always be non-negative or null.' }],
  ['topic', null, { name: 'IllegalArgumentError', message: 'Topic cannot be null.' }],
  ['topic', 7, { name: 'TypeError', message: 'topic must be a string' }],
  ['topic', undefined, { name: 'TypeError', message: "topic must not be undefined. Leave the key out, or pass null for Java's null" }],
];

/** A valid record, then `field` reassigned to `value` from plain JavaScript. */
function reassigned(field, value) {
  const r = new ProducerRecord({ topic: 't', partition: 0, timestamp: 1, key: 'k', value: 'v', headers: [H('h', bytes('x'))] });
  r[field] = value;
  return r;
}

test('a field reassigned after the constructor is checked again at send, with the constructor\'s error', () => {
  for (const [field, value, expected] of REASSIGNED) {
    assert.throws(() => reassigned(field, value).internalToNative(), expected, `${field} = ${String(value)}`);
  }
  // The constructor gives each value the same error (where it takes the value
  // as given: it reads a parameter left out as null, and refuses undefined in
  // its form check, with the same messages).
  for (const [field, value, expected] of REASSIGNED) {
    // The constructor takes any key and value (the serializers' input).
    if (value === undefined || field === 'key' || field === 'value') continue;
    const options = { topic: 't', partition: 0, timestamp: 1, key: 'k', value: 'v', headers: [H('h', bytes('x'))], [field]: value };
    assert.throws(() => new ProducerRecord(options), expected, `new ProducerRecord({${field}: ${String(value)}})`);
  }
});

test('a field reassigned to a valid value is sent as reassigned', () => {
  assert.deepEqual(reassigned('headers', null).internalToNative()[6], null);
  assert.deepEqual(reassigned('partition', null).internalToNative()[1], -1);
  assert.deepEqual(reassigned('value', null).internalToNative()[4], null);
  // An iterable that is not a Headers is read as the constructor reads it.
  const set = reassigned('headers', new Set([H('a', null)]));
  assert.deepEqual(keysAsText(set.internalToNative()[6]), [['a', null]]);
  const other = reassigned('headers', new Headers([H('b', null)]));
  assert.deepEqual(keysAsText(other.internalToNative()[6]), [['b', null]]);
  // A Header keeps its state in private fields: nothing to reassign.
  const header = H('h', null);
  header.value = 'x';
  const withHeader = new ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: [header] });
  assert.deepEqual(keysAsText(withHeader.internalToNative()[6]), [['h', null]]);
});

test('toString follows Java', () => {
  const r = new ProducerRecord({ topic: 't', partition: null, key: 'k', value: Buffer.from([1, 255]), headers: [H('h', null)] });
  assert.equal(
    r.toString(),
    'ProducerRecord(topic=t, partition=null, headers=RecordHeaders(headers = [RecordHeader(key = h, value = null)], ' +
      'isReadOnly = false), key=k, value=[1, -1], timestamp=null)',
  );
  // A header value is bytes, printed as Arrays.toString(value()) does; a
  // string key or value prints as text.
  const s = new ProducerRecord({
    topic: 't',
    partition: 2,
    timestamp: 5,
    key: 'k',
    value: 'v',
    headers: [
      H('a', bytes('vé')),
      new Header({ key: 'b', value: new Uint8Array([118]) }),
    ],
  });
  assert.equal(
    s.toString(),
    'ProducerRecord(topic=t, partition=2, headers=RecordHeaders(headers = [RecordHeader(key = a, value = [118, -61, -87]), ' +
      'RecordHeader(key = b, value = [118])], isReadOnly = false), key=k, value=v, timestamp=5)',
  );
});
