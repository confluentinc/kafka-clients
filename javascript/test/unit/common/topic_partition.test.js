// Copyright 2026 Confluent Inc.
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

// Java's TopicPartitionTest checks Java serialization (a round trip through
// ObjectOutputStream, and a file serialized by an older client); JavaScript
// has no such serialization, so its checkValues assertions are made on a
// constructed TopicPartition, and the JSON form (js-only) takes the place of
// the round trip.

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { TopicPartition } = require('@confluentinc/kafka-javascript/common');

const topicName = 'mytopic';
const partNum = 5;

function checkValues(tp) {
  assert.equal(tp.partition, partNum, `partition number should be ${partNum} but got ${tp.partition}`);
  assert.equal(tp.topic, topicName, `topic should be ${topicName} but got ${tp.topic}`);
}

test('testSerializationRoundtrip, as a JSON round trip', () => {
  const origTp = new TopicPartition({ topic: topicName, partition: partNum });
  checkValues(origTp);
  const json = JSON.parse(JSON.stringify(origTp));
  assert.deepEqual(json, { topic: topicName, partition: partNum });
  checkValues(new TopicPartition(json));
});

test('a data object, frozen when created, with Java toString()', () => {
  const tp = new TopicPartition({ topic: 'foo', partition: 3 });
  assert.deepEqual(Object.keys(tp), ['topic', 'partition']);
  for (const name of ['topic', 'partition']) {
    const d = Object.getOwnPropertyDescriptor(tp, name);
    assert.ok('value' in d && d.enumerable, `${name} is an own enumerable data property`);
  }
  assert.equal(Object.isFrozen(tp), true);
  assert.throws(() => {
    tp.partition = 4;
  }, TypeError);
  assert.equal(tp.partition, 3);
  assert.equal(tp.toString(), 'foo-3');
  assert.equal(String(tp), 'foo-3');
  assert.deepEqual({ ...tp }, { topic: 'foo', partition: 3 });
  // Java accepts a null topic, and writes it as "null".
  const nullTopic = new TopicPartition({ topic: null, partition: 0 });
  assert.equal(nullTopic.topic, null);
  assert.equal(nullTopic.toString(), 'null-0');
});

test('the constructor takes Java form only, with run-time types', () => {
  assert.throws(() => new TopicPartition({ topic: 't' }), {
    constructor: IllegalArgumentError,
    message: 'TopicPartition() takes one of (topic, partition); got (topic)',
  });
  assert.throws(() => new TopicPartition({ topic: 't', partition: 0, offset: 1 }), {
    constructor: IllegalArgumentError,
    message: 'TopicPartition() takes one of (topic, partition); got (topic, partition, offset)',
  });
  assert.throws(() => new TopicPartition({ topic: 't', partition: 0 }, 1), {
    constructor: IllegalArgumentError,
    message: 'TopicPartition() takes one of (topic, partition); got 2 arguments, not one object of named parameters',
  });
  assert.throws(() => new TopicPartition('t-0'), {
    constructor: IllegalArgumentError,
    message: 'TopicPartition() takes one of (topic, partition); got a string',
  });
  assert.throws(() => new TopicPartition({ topic: 't', partition: undefined }), {
    constructor: TypeError,
    message: "partition must not be undefined. Leave the key out, or pass null for Java's null",
  });
  assert.throws(() => new TopicPartition({ topic: 1, partition: 0 }), { constructor: TypeError, message: 'topic must be a string or null' });
  assert.throws(() => new TopicPartition({ topic: 't', partition: 0.5 }), { constructor: TypeError, message: 'partition must be an integer' });
  assert.throws(() => new TopicPartition({ topic: 't', partition: null }), { constructor: TypeError, message: 'partition must be an integer' });
});
