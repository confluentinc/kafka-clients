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

// Java's PartitionInfoTest. Java's public constructors take Node[] arrays,
// whose JavaScript type is not decided yet (Q25), so the test builds the
// PartitionInfo through the client's library-only factory.

const test = require('node:test');
const assert = require('node:assert/strict');

const { Node, PartitionInfo } = require('@confluentinc/kafka-javascript/common');

test('testToString', () => {
  const topic = 'sample';
  const partition = 0;
  const leader = new Node({ id: 0, host: 'localhost', port: 9092 });
  const r1 = new Node({ id: 1, host: 'localhost', port: 9093 });
  const r2 = new Node({ id: 2, host: 'localhost', port: 9094 });
  const replicas = [leader, r1, r2];
  const inSyncReplicas = [leader, r1];
  const offlineReplicas = [r2];
  const partitionInfo = PartitionInfo.internalCreate({ topic, partition, leader, replicas, inSyncReplicas, offlineReplicas });

  const expected = `Partition(topic = ${topic}, partition = ${partition}, leader = ${leader.idString}, replicas = [0,1,2], isr = [0,1], offlineReplicas = [2])`;
  assert.equal(partitionInfo.toString(), expected);
});

test('no leader is "none", and a null array is empty brackets, as Java writes them', () => {
  const info = PartitionInfo.internalCreate({ topic: 't', partition: 3, leader: null, replicas: [], inSyncReplicas: null, offlineReplicas: [] });
  assert.equal(info.toString(), 'Partition(topic = t, partition = 3, leader = none, replicas = [], isr = [], offlineReplicas = [])');
});

test("Java's getters stay methods, which only read", () => {
  const leader = new Node({ id: 4, host: 'h', port: 1 });
  const info = PartitionInfo.internalCreate({ topic: 't', partition: 2, leader, replicas: [leader], inSyncReplicas: [leader], offlineReplicas: [] });
  assert.equal(info.topic(), 't');
  assert.equal(info.partition(), 2);
  assert.equal(info.leader(), leader);
  assert.deepEqual(Object.keys(info), []);
  for (const name of ['topic', 'partition', 'leader']) {
    assert.throws(() => info[name](1), { constructor: TypeError, message: `${name}() only reads: it takes no argument` });
  }
});
