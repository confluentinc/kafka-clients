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

// Java's RecordMetadataTest, on Java's public constructor; then the
// metadata the library's own factory builds from the fields the native layer
// reads (the offset already summed by the core, hasOffset false where Java's
// base offset is -1).

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { TopicPartition } = require('@confluentinc/kafka-javascript/common');
const { RecordMetadata } = require('@confluentinc/kafka-javascript/producer');

const FIELDS = ['topic', 'partition', 'offset', 'hasOffset', 'timestamp', 'hasTimestamp', 'serializedKeySize', 'serializedValueSize'];

function metadataOf(fields) {
  return RecordMetadata.internalCreate({ hasTimestamp: true, ...fields });
}

test('testConstructionWithMissingBatchIndex', () => {
  const tp = new TopicPartition({ topic: 'foo', partition: 0 });
  const timestamp = 2340234n;
  const keySize = 3;
  const valueSize = 5;

  const metadata = new RecordMetadata({ topicPartition: tp, baseOffset: -1n, batchIndex: -1, timestamp, serializedKeySize: keySize, serializedValueSize: valueSize });
  assert.equal(metadata.topic, tp.topic);
  assert.equal(metadata.partition, tp.partition);
  assert.equal(metadata.timestamp, timestamp);
  assert.equal(metadata.hasOffset, false);
  assert.equal(metadata.offset, -1n);
  assert.equal(metadata.serializedKeySize, keySize);
  assert.equal(metadata.serializedValueSize, valueSize);
});

test('testConstructionWithBatchIndexOffset', () => {
  const tp = new TopicPartition({ topic: 'foo', partition: 0 });
  const timestamp = 2340234n;
  const keySize = 3;
  const valueSize = 5;
  const baseOffset = 15n;
  const batchIndex = 3;

  const metadata = new RecordMetadata({ topicPartition: tp, baseOffset, batchIndex, timestamp, serializedKeySize: keySize, serializedValueSize: valueSize });
  assert.equal(metadata.topic, tp.topic);
  assert.equal(metadata.partition, tp.partition);
  assert.equal(metadata.timestamp, timestamp);
  assert.equal(metadata.offset, baseOffset + BigInt(batchIndex));
  assert.equal(metadata.serializedKeySize, keySize);
  assert.equal(metadata.serializedValueSize, valueSize);
});

test("the public constructor: a long is a bigint or a whole number, and the fields are Java's", () => {
  const tp = new TopicPartition({ topic: 'foo', partition: 0 });
  const args = { topicPartition: tp, baseOffset: 15, batchIndex: 3, timestamp: 2340234, serializedKeySize: 3, serializedValueSize: 5 };
  const metadata = new RecordMetadata(args);
  // Java: new RecordMetadata(new TopicPartition("foo", 0), 15L, 3, 2340234L, 3, 5) prints foo-0@18.
  assert.equal(metadata.toString(), 'foo-0@18');
  assert.equal(metadata.offset, 18n);
  assert.equal(metadata.timestamp, 2340234n);
  assert.equal(metadata.hasTimestamp, true);
  assert.equal(Object.isFrozen(metadata), true);
  // Java's NO_TIMESTAMP is -1: no timestamp.
  assert.equal(new RecordMetadata({ ...args, timestamp: -1 }).hasTimestamp, false);
  // Java's long arithmetic wraps.
  assert.equal(new RecordMetadata({ ...args, baseOffset: 2n ** 63n - 1n, batchIndex: 1 }).offset, -(2n ** 63n));
  assert.throws(() => new RecordMetadata({ ...args, baseOffset: 2 ** 53 }), {
    constructor: RangeError,
    message: 'baseOffset must be a bigint beyond 2^53 - 1: a number holds no larger integer exactly',
  });
  assert.throws(() => new RecordMetadata({ ...args, timestamp: 2n ** 63n }), {
    constructor: RangeError,
    message: 'timestamp must be from -9223372036854775808 to 9223372036854775807',
  });
  assert.throws(() => new RecordMetadata({ ...args, timestamp: 1.5 }), { constructor: TypeError, message: 'timestamp must be a bigint or an integer' });
  assert.throws(() => new RecordMetadata({ ...args, batchIndex: 2 ** 31 }), {
    constructor: RangeError,
    message: 'batchIndex must be from -2147483648 to 2147483647',
  });
  assert.throws(() => new RecordMetadata({ ...args, topicPartition: null }), { constructor: TypeError, message: 'topicPartition must not be null' });
  assert.throws(() => new RecordMetadata({ ...args, topicPartition: { topic: 'foo', partition: 0 } }), {
    constructor: TypeError,
    message: 'topicPartition must be a TopicPartition',
  });
  assert.throws(() => new RecordMetadata({ topicPartition: tp }), {
    constructor: IllegalArgumentError,
    message: 'RecordMetadata() takes one of (topicPartition, baseOffset, batchIndex, timestamp, serializedKeySize, serializedValueSize); got (topicPartition)',
  });
});

test('no timestamp reads -1, as Java returns it', () => {
  const metadata = metadataOf({ topic: 'foo', partition: 0, hasOffset: true, offset: 1n, hasTimestamp: false, timestamp: 7n, serializedKeySize: -1, serializedValueSize: -1 });
  assert.equal(metadata.hasTimestamp, false);
  assert.equal(metadata.timestamp, -1n);
});

// A data object: Java's getters are own enumerable fields set once, so
// spread, JSON.stringify and structuredClone keep them.
test('the fields are own data properties, in Java order, kept by spread, JSON and structuredClone', () => {
  const metadata = metadataOf({ topic: 'foo', partition: 2, hasOffset: true, offset: 9n, timestamp: 4n, serializedKeySize: -1, serializedValueSize: 5 });
  assert.deepEqual(Object.keys(metadata), FIELDS);
  for (const name of FIELDS) {
    const d = Object.getOwnPropertyDescriptor(metadata, name);
    assert.ok('value' in d && d.enumerable, `${name} is an own enumerable data property`);
    assert.equal(Object.getOwnPropertyDescriptor(RecordMetadata.prototype, name), undefined);
  }
  const expected = { topic: 'foo', partition: 2, offset: 9n, hasOffset: true, timestamp: 4n, hasTimestamp: true, serializedKeySize: -1, serializedValueSize: 5 };
  assert.deepEqual({ ...metadata }, expected);
  // toJSON writes Java's longs as decimal strings, which BigInt() reads back.
  assert.deepEqual(JSON.parse(JSON.stringify(metadata)), { ...expected, offset: '9', timestamp: '4' });
  assert.deepEqual(structuredClone(metadata), expected);
  assert.equal(Object.isFrozen(metadata), true);
  assert.equal(metadata.toString(), 'foo-2@9');
  // A long beyond 2^53 is exact, in the field and in JSON.
  const big = metadataOf({ topic: 'foo', partition: 0, hasOffset: true, offset: 2n ** 62n + 1n, timestamp: 1n, serializedKeySize: -1, serializedValueSize: -1 });
  assert.equal(big.offset, 4611686018427387905n);
  assert.equal(JSON.parse(JSON.stringify(big)).offset, '4611686018427387905');
  assert.equal(RecordMetadata.UNKNOWN_PARTITION, -1);
});
