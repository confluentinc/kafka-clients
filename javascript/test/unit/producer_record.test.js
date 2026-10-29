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

const { KafkaError, ProducerRecord } = require('../..');

test('the fields Java leaves out default to null and no headers', () => {
  const r = new ProducerRecord({ topic: 't', value: 'v' });
  assert.equal(r.topic, 't');
  assert.equal(r.partition, null);
  assert.equal(r.timestamp, null);
  assert.equal(r.key, null);
  assert.equal(r.value, 'v');
  assert.deepEqual(r.headers, []);
});

test("Java's constructor checks keep Java's messages", () => {
  for (const topic of [undefined, null]) {
    assert.throws(() => new ProducerRecord({ topic }), (e) => {
      assert.ok(e instanceof KafkaError);
      assert.equal(e.name, 'LOCAL_ILLEGAL_ARGUMENT');
      assert.equal(e.code, -3);
      assert.equal(e.message, 'Topic cannot be null.');
      return true;
    });
  }
  assert.throws(() => new ProducerRecord({ topic: 't', timestamp: -1 }), {
    name: 'LOCAL_ILLEGAL_ARGUMENT',
    message: 'Invalid timestamp: -1. Timestamp should always be non-negative or null.',
  });
  assert.throws(() => new ProducerRecord({ topic: 't', partition: -2 }), {
    name: 'LOCAL_ILLEGAL_ARGUMENT',
    message: 'Invalid partition: -2. Partition number should always be non-negative or null.',
  });
  assert.throws(() => new ProducerRecord({ topic: 't', headers: [{ value: 'x' }] }), {
    name: 'TypeError',
    message: 'Null header keys are not permitted',
  });
});

test('values of the wrong type are TypeErrors', () => {
  assert.throws(() => new ProducerRecord(), { name: 'TypeError' });
  assert.throws(() => new ProducerRecord({ topic: 1 }), { name: 'TypeError', message: 'topic must be a string' });
  assert.throws(() => new ProducerRecord({ topic: 't', key: 1 }), {
    name: 'TypeError',
    message: 'key must be a Buffer, a Uint8Array, a string or null',
  });
  assert.throws(() => new ProducerRecord({ topic: 't', value: {} }), {
    name: 'TypeError',
    message: 'value must be a Buffer, a Uint8Array, a string or null',
  });
  assert.throws(() => new ProducerRecord({ topic: 't', partition: 1.5 }), {
    name: 'TypeError',
    message: 'partition must be an integer or null',
  });
  assert.throws(() => new ProducerRecord({ topic: 't', partition: 2 ** 31 }), { name: 'RangeError' });
  assert.throws(() => new ProducerRecord({ topic: 't', headers: 'x' }), { name: 'TypeError' });
});

test('key and value cross as bytes without copying a Buffer', () => {
  const key = Buffer.from('k');
  const value = new Uint8Array([1, 2, 3]);
  const r = new ProducerRecord({ topic: 't', partition: 3, timestamp: 1000, key, value });
  const [topic, partition, timestamp, k, v, headers] = r._toNative();
  assert.equal(topic, 't');
  assert.equal(partition, 3);
  assert.equal(timestamp, 1000);
  assert.equal(k, key);
  assert.equal(v, value);
  assert.equal(headers, null);
});

test('strings are encoded as UTF-8 and null stays null', () => {
  const r = new ProducerRecord({ topic: 't', key: 'clé', value: null, headers: [{ key: 'h', value: 'é' }, { key: 'n', value: null }] });
  const [, partition, timestamp, k, v, headers] = r._toNative();
  assert.equal(partition, -1);
  assert.equal(timestamp, -1);
  assert.deepEqual(k, Buffer.from('clé', 'utf8'));
  assert.equal(v, null);
  assert.deepEqual(headers, [['h', Buffer.from('é', 'utf8')], ['n', null]]);
});

test('toString follows Java', () => {
  const r = new ProducerRecord({ topic: 't', key: 'k', value: Buffer.from([1, 255]), headers: [{ key: 'h', value: null }] });
  assert.equal(
    r.toString(),
    'ProducerRecord(topic=t, partition=null, headers=RecordHeaders(headers = [RecordHeader(key = h, value = null)], ' +
      'isReadOnly = false), key=k, value=[1, -1], timestamp=null)',
  );
});
