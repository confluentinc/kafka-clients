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

// TopicPartitionInfo: its JSON form (Java's TopicPartitionInfoTest is not in
// the clients module).

const assert = require('node:assert/strict');

const { Node, TopicPartitionInfo } = require('@confluentinc/kafka-javascript/common');

test('the JSON form: the fields, the nodes by their fields', () => {
  const leader = new Node({ id: 1, host: 'h', port: 9092 });
  const info = new TopicPartitionInfo({ partition: 0, leader, replicas: [leader], isr: [] });
  const json = JSON.parse(JSON.stringify(info));
  assert.equal(json.partition, 0);
  assert.equal(json.leader.id, 1);
  assert.equal(json.replicas.length, 1);
  assert.deepEqual(json.isr, []);
  assert.equal(json.elr, null);
  assert.equal(json.lastKnownElr, null);
});
