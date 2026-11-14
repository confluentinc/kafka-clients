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

// Java's RecordMetadataTest. Java builds the metadata from a TopicPartition, a
// base offset and a batch index; here the client builds it through its
// library-only factory from the fields the native layer reads, the offset
// already summed by the core (hasOffset false where Java's base offset is -1).

const test = require('node:test');
const assert = require('node:assert/strict');

const { RecordMetadata } = require('@confluentinc/kafka-javascript/producer');

const FIELDS = ['topic', 'partition', 'offset', 'hasOffset', 'timestamp', 'hasTimestamp', 'serializedKeySize', 'serializedValueSize'];

function metadataOf(fields) {
  return RecordMetadata.internalCreate({ hasTimestamp: true, ...fields });
}

test('testConstructionWithMissingBatchIndex', () => {
  const timestamp = 2340234n;
  const metadata = metadataOf({ topic: 'foo', partition: 0, hasOffset: false, offset: 0n, timestamp, serializedKeySize: 3, serializedValueSize: 5 });
  assert.equal(metadata.topic, 'foo');
  assert.equal(metadata.partition, 0);
  assert.equal(metadata.timestamp, timestamp);
  assert.equal(metadata.hasOffset, false);
  assert.equal(metadata.offset, -1n);
  assert.equal(metadata.serializedKeySize, 3);
  assert.equal(metadata.serializedValueSize, 5);
});

test('testConstructionWithBatchIndexOffset', () => {
  const timestamp = 2340234n;
  const metadata = metadataOf({ topic: 'foo', partition: 0, hasOffset: true, offset: 15n + 3n, timestamp, serializedKeySize: 3, serializedValueSize: 5 });
  assert.equal(metadata.topic, 'foo');
  assert.equal(metadata.partition, 0);
  assert.equal(metadata.timestamp, timestamp);
  assert.equal(metadata.offset, 18n);
  assert.equal(metadata.serializedKeySize, 3);
  assert.equal(metadata.serializedValueSize, 5);
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
  assert.equal(Object.isFrozen(metadata), false);
  assert.equal(metadata.toString(), 'foo-2@9');
  // A long beyond 2^53 is exact, in the field and in JSON.
  const big = metadataOf({ topic: 'foo', partition: 0, hasOffset: true, offset: 2n ** 62n + 1n, timestamp: 1n, serializedKeySize: -1, serializedValueSize: -1 });
  assert.equal(big.offset, 4611686018427387905n);
  assert.equal(JSON.parse(JSON.stringify(big)).offset, '4611686018427387905');
  assert.equal(RecordMetadata.UNKNOWN_PARTITION, -1);
});
