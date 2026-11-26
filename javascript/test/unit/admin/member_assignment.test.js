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

// MemberAssignment: Java has no test of it. These check Java's copy of the
// set, its null reading as none, equals, hashCode, toString and the
// TopicPartitionSet topicPartitions() returns.

const assert = require('node:assert/strict');

const { MemberAssignment } = require('@confluentinc/kafka-javascript/admin');
const { TopicPartition, TopicPartitionSet } = require('@confluentinc/kafka-javascript/common');

const tp0 = new TopicPartition({ topic: 't', partition: 0 });
const tp1 = new TopicPartition({ topic: 't', partition: 1 });

test("Java's Set.copyOf: any iterable, equal partitions once, null none, a null element refused", () => {
  const a = new MemberAssignment([tp0, tp1, new TopicPartition({ topic: 't', partition: 0 })]);
  const partitions = a.topicPartitions();
  assert.ok(partitions instanceof TopicPartitionSet);
  assert.equal(partitions.size(), 2);
  assert.notEqual(a.topicPartitions(), partitions);
  assert.equal(new MemberAssignment(null).topicPartitions().size(), 0);
  assert.equal(new MemberAssignment({ topicPartitions: new Set([tp1]) }).topicPartitions().size(), 1);
  // @ts-expect-error a null element, which Java's Set.copyOf refuses
  assert.throws(() => new MemberAssignment([null]), { name: 'TypeError', message: 'topicPartitions must not contain null' });
  assert.equal(String(a), '(topicPartitions=t-0,t-1)');
  assert.equal(JSON.stringify(new MemberAssignment([tp0])), '{"topicPartitions":[{"topic":"t","partition":0}]}');
});

test("equals and hashCode are Java's Set's, whatever the order", () => {
  const a = new MemberAssignment([tp0, tp1]);
  const b = new MemberAssignment([tp1, tp0]);
  assert.ok(a.equals(b));
  assert.equal(a.hashCode(), b.hashCode());
  assert.equal(a.hashCode(), (tp0.hashCode() + tp1.hashCode()) | 0);
  assert.ok(!a.equals(new MemberAssignment([tp0])));
});
