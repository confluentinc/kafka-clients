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

// TopicPartitionReplica (Java has no test of its own for it: these check
// Java's equals, hashCode and toString, its null check and the forms) and
// TopicPartitionReplicaMap, the js-only map keyed by TopicPartitionReplica
// (root CLAUDE.md, JavaScript Binding Conventions, Class family, Q25;
// DECISIONS.md entry 38), which stands for Java's HashMap<TopicPartitionReplica, V>.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { TopicPartitionReplica, TopicPartitionReplicaMap } = require('@confluentinc/kafka-javascript/common');

/**
 * @param {string} topic
 * @param {number} partition
 * @param {number} brokerId
 */
const replica = (topic, partition, brokerId) => new TopicPartitionReplica({ topic, partition, brokerId });

test("TopicPartitionReplica keeps Java's equals, hashCode, toString and null check", () => {
  const r = replica('orders', 3, 1);
  assert.equal(r.topic(), 'orders');
  assert.equal(r.partition(), 3);
  assert.equal(r.brokerId(), 1);
  assert.equal(r.toString(), 'orders-3-1');
  assert.equal(r.equals(replica('orders', 3, 1)), true);
  assert.equal(r.equals({ obj: replica('orders', 3, 1) }), true);
  assert.equal(r.equals(replica('orders', 3, 2)), false);
  assert.equal(r.equals(replica('orders', 4, 1)), false);
  assert.equal(r.equals(replica('payments', 3, 1)), false);
  assert.equal(r.equals(null), false);
  // Java's 31 * (31 * (31 + "orders".hashCode()) + 3) + 1.
  let h = 1;
  let s = 0;
  for (const c of 'orders') s = (Math.imul(31, s) + c.charCodeAt(0)) | 0;
  h = (Math.imul(31, h) + s) | 0;
  h = (Math.imul(31, h) + 3) | 0;
  h = (Math.imul(31, h) + 1) | 0;
  assert.equal(r.hashCode(), h);
  assert.equal(JSON.stringify(r), '{"topic":"orders","partition":3,"brokerId":1}');
  // @ts-expect-error a null topic, which the types forbid: Java's Objects.requireNonNull
  assert.throws(() => replica(null, 0, 0), { name: 'TypeError', message: 'topic must not be null' });
  assert.throws(() => replica('t', 2 ** 31, 0), { name: 'RangeError', message: 'partition must be from -2147483648 to 2147483647' });
  // @ts-expect-error a key no form has: the form check
  assert.throws(() => new TopicPartitionReplica({ topic: 't', partition: 0 }), IllegalArgumentError);
  // @ts-expect-error an argument given to a reader: it only reads
  assert.throws(() => r.topic(1), { name: 'TypeError', message: 'topic() only reads: it takes no argument' });
});

test('a TopicPartitionReplicaMap is keyed by TopicPartitionReplica.equals, keeping the original keys', () => {
  const first = replica('orders', 0, 1);
  const map = new TopicPartitionReplicaMap([
    [first, '/a'],
    [replica('orders', 0, 2), '/b'],
  ]);
  assert.equal(map.size(), 2);
  assert.equal(map.get(replica('orders', 0, 1)), '/a');
  assert.equal(map.has(replica('orders', 1, 1)), false);
  assert.equal([...map.keys()][0], first);
  assert.equal(map.toString(), '{orders-0-1=/a, orders-0-2=/b}');
  assert.equal(JSON.stringify(map), '{"[\\"orders\\",0,1]":"/a","[\\"orders\\",0,2]":"/b"}');
  assert.equal(map.delete({ key: replica('orders', 0, 2) }), true);
  assert.deepEqual([...map.values()], ['/a']);
  assert.equal(map instanceof Map, false);
});
