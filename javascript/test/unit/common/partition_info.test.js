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

// Java's PartitionInfoTest, and the members Java's class has beside
// toString(): its constructors, which take any iterable of Node for Java's
// Node[], the array getters, equals and hashCode. The hash codes are Java's,
// computed by Java's PartitionInfo (kafka-clients) on the same values.

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { Node, PartitionInfo } = require('@confluentinc/kafka-javascript/common');

const FORMS =
  'PartitionInfo() takes one of (topic, partition, leader, replicas, inSyncReplicas), ' +
  '(topic, partition, leader, replicas, inSyncReplicas, offlineReplicas)';

test('testToString', () => {
  const topic = 'sample';
  const partition = 0;
  const leader = new Node({ id: 0, host: 'localhost', port: 9092 });
  const r1 = new Node({ id: 1, host: 'localhost', port: 9093 });
  const r2 = new Node({ id: 2, host: 'localhost', port: 9094 });
  const replicas = [leader, r1, r2];
  const inSyncReplicas = [leader, r1];
  const offlineReplicas = [r2];
  const partitionInfo = new PartitionInfo({ topic, partition, leader, replicas, inSyncReplicas, offlineReplicas });

  const expected = `Partition(topic = ${topic}, partition = ${partition}, leader = ${leader.idString}, replicas = [0,1,2], isr = [0,1], offlineReplicas = [2])`;
  assert.equal(partitionInfo.toString(), expected);
});

test('no leader is "none", and a null array is empty brackets, as Java writes them', () => {
  const info = new PartitionInfo({ topic: 't', partition: 3, leader: null, replicas: [], inSyncReplicas: null, offlineReplicas: [] });
  assert.equal(info.toString(), 'Partition(topic = t, partition = 3, leader = none, replicas = [], isr = [], offlineReplicas = [])');
});

test("Java's getters stay methods, which only read; the arrays are new arrays each call", () => {
  const leader = new Node({ id: 4, host: 'h', port: 1 });
  const other = new Node({ id: 5, host: 'h', port: 2 });
  const replicas = [leader, other];
  const info = new PartitionInfo({ topic: 't', partition: 2, leader, replicas, inSyncReplicas: new Set([leader]) });
  assert.equal(info.topic(), 't');
  assert.equal(info.partition(), 2);
  assert.equal(info.leader(), leader);
  assert.deepEqual(info.replicas(), [leader, other]);
  assert.notEqual(info.replicas(), info.replicas());
  // The constructor copies the iterable: changing it, or a returned array,
  // changes nothing.
  replicas.pop();
  info.replicas().pop();
  assert.deepEqual(info.replicas(), [leader, other]);
  assert.deepEqual(info.inSyncReplicas(), [leader]);
  // Java's five-parameter constructor passes new Node[0].
  assert.deepEqual(info.offlineReplicas(), []);
  assert.equal(new PartitionInfo({ topic: 't', partition: 0, leader: null, replicas: null, inSyncReplicas: null, offlineReplicas: null }).replicas(), null);
  assert.deepEqual(Object.keys(info), []);
  for (const name of ['topic', 'partition', 'leader', 'replicas', 'inSyncReplicas', 'offlineReplicas', 'hashCode']) {
    assert.throws(() => info[name](1), { constructor: TypeError, message: `${name}() only reads: it takes no argument` });
  }
});

test("equals and hashCode are Java's: the arrays element by element", () => {
  const n0 = new Node({ id: 0, host: 'localhost', port: 9092 });
  const n1 = new Node({ id: 1, host: 'localhost', port: 9093, rack: 'rack1' });
  const r2 = new Node({ id: 2, host: 'localhost', port: 9094 });
  const p1 = new PartitionInfo({ topic: 'sample', partition: 0, leader: n0, replicas: [n0, n1, r2], inSyncReplicas: [n0, n1], offlineReplicas: [r2] });
  const p2 = new PartitionInfo({ topic: 'sample', partition: 0, leader: null, replicas: null, inSyncReplicas: null, offlineReplicas: null });
  const p3 = new PartitionInfo({ topic: 't', partition: 3, leader: n0, replicas: [n0], inSyncReplicas: [n0] });
  // Java: new PartitionInfo(...).hashCode() on the same values.
  assert.equal(p1.hashCode(), -670058797);
  assert.equal(p2.hashCode(), 949469143);
  assert.equal(p3.hashCode(), -350980442);
  const same = new PartitionInfo({
    topic: 't',
    partition: 3,
    leader: new Node({ id: 0, host: 'localhost', port: 9092 }),
    replicas: [new Node({ id: 0, host: 'localhost', port: 9092 })],
    inSyncReplicas: [n0],
    offlineReplicas: [],
  });
  assert.equal(p3.equals(same), true);
  assert.equal(p3.equals({ obj: same }), true);
  assert.equal(same.hashCode(), p3.hashCode());
  // A null array is not an empty one.
  assert.equal(p2.equals(new PartitionInfo({ topic: 'sample', partition: 0, leader: null, replicas: null, inSyncReplicas: null, offlineReplicas: [] })), false);
  assert.equal(p1.equals(p1), true);
  assert.equal(p1.equals(null), false);
  assert.equal(p1.equals('sample'), false);
  assert.throws(() => p1.equals(p1, p1), {
    constructor: IllegalArgumentError,
    message: 'equals() takes one of (obj); got 2 arguments, not one object of named parameters',
  });
});

test('the constructor takes Java forms only, with run-time types', () => {
  const node = new Node({ id: 1, host: 'h', port: 1 });
  assert.throws(() => new PartitionInfo({ topic: 't', partition: 0 }), {
    constructor: IllegalArgumentError,
    message: `${FORMS}; got (topic, partition)`,
  });
  const base = { topic: 't', partition: 0, leader: node, replicas: [node], inSyncReplicas: [node] };
  assert.throws(() => new PartitionInfo({ ...base, topic: 1 }), { constructor: TypeError, message: 'topic must be a string' });
  assert.throws(() => new PartitionInfo({ ...base, partition: 0.5 }), { constructor: TypeError, message: 'partition must be an integer' });
  assert.throws(() => new PartitionInfo({ ...base, partition: 2 ** 31 }), { constructor: RangeError, message: 'partition must be from -2147483648 to 2147483647' });
  assert.throws(() => new PartitionInfo({ ...base, leader: { id: 1 } }), { constructor: TypeError, message: 'leader must be a Node or null' });
  assert.throws(() => new PartitionInfo({ ...base, replicas: 5 }), { constructor: TypeError, message: 'replicas must be an iterable of Node, or null' });
  assert.throws(() => new PartitionInfo({ ...base, inSyncReplicas: [node, 'n'] }), { constructor: TypeError, message: 'inSyncReplicas must hold only Node, not a string' });
});
