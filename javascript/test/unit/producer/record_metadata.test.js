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
  const timestamp = 2340234;
  const metadata = metadataOf({ topic: 'foo', partition: 0, hasOffset: false, offset: 0, timestamp, serializedKeySize: 3, serializedValueSize: 5 });
  assert.equal(metadata.topic, 'foo');
  assert.equal(metadata.partition, 0);
  assert.equal(metadata.timestamp, timestamp);
  assert.equal(metadata.hasOffset, false);
  assert.equal(metadata.offset, -1);
  assert.equal(metadata.serializedKeySize, 3);
  assert.equal(metadata.serializedValueSize, 5);
});

test('testConstructionWithBatchIndexOffset', () => {
  const timestamp = 2340234;
  const metadata = metadataOf({ topic: 'foo', partition: 0, hasOffset: true, offset: 15 + 3, timestamp, serializedKeySize: 3, serializedValueSize: 5 });
  assert.equal(metadata.topic, 'foo');
  assert.equal(metadata.partition, 0);
  assert.equal(metadata.timestamp, timestamp);
  assert.equal(metadata.offset, 18);
  assert.equal(metadata.serializedKeySize, 3);
  assert.equal(metadata.serializedValueSize, 5);
});

test('no timestamp reads -1, as Java returns it', () => {
  const metadata = metadataOf({ topic: 'foo', partition: 0, hasOffset: true, offset: 1, hasTimestamp: false, timestamp: 7, serializedKeySize: -1, serializedValueSize: -1 });
  assert.equal(metadata.hasTimestamp, false);
  assert.equal(metadata.timestamp, -1);
});

// A data object: Java's getters are own enumerable fields set once, so
// spread, JSON.stringify and structuredClone keep them.
test('the fields are own data properties, in Java order, kept by spread, JSON and structuredClone', () => {
  const metadata = metadataOf({ topic: 'foo', partition: 2, hasOffset: true, offset: 9, timestamp: 4, serializedKeySize: -1, serializedValueSize: 5 });
  assert.deepEqual(Object.keys(metadata), FIELDS);
  for (const name of FIELDS) {
    const d = Object.getOwnPropertyDescriptor(metadata, name);
    assert.ok('value' in d && d.enumerable, `${name} is an own enumerable data property`);
    assert.equal(Object.getOwnPropertyDescriptor(RecordMetadata.prototype, name), undefined);
  }
  const expected = { topic: 'foo', partition: 2, offset: 9, hasOffset: true, timestamp: 4, hasTimestamp: true, serializedKeySize: -1, serializedValueSize: 5 };
  assert.deepEqual({ ...metadata }, expected);
  assert.deepEqual(JSON.parse(JSON.stringify(metadata)), expected);
  assert.deepEqual(structuredClone(metadata), expected);
  assert.equal(Object.isFrozen(metadata), false);
  assert.equal(metadata.toString(), 'foo-2@9');
  assert.equal(RecordMetadata.UNKNOWN_PARTITION, -1);
});
