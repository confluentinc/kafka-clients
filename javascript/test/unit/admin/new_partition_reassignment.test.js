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

// NewPartitionReassignment, a value class: Java's constructor refuses no
// replicas, and copies the list. Java has no test of it.

const assert = require('node:assert/strict');

const { NewPartitionReassignment } = require('@confluentinc/kafka-javascript/admin');

test('NewPartitionReassignment: the target replicas, copied from any iterable', () => {
  const given = [1, 2];
  const r = new NewPartitionReassignment(given);
  given.push(3);
  assert.deepEqual(r.targetReplicas(), [1, 2]);
  assert.notEqual(r.targetReplicas(), r.targetReplicas());
  assert.deepEqual(new NewPartitionReassignment({ targetReplicas: new Set([4]) }).targetReplicas(), [4]);
  assert.equal(JSON.stringify(r), '{"targetReplicas":[1,2]}');
});

test("NewPartitionReassignment: Java's IllegalArgumentException for no replicas, a TypeError for a null one", () => {
  for (const replicas of [[], null]) {
    // @ts-expect-error a null the types forbid: Java's constructor checks it
    assert.throws(() => new NewPartitionReassignment({ targetReplicas: replicas }), {
      name: 'IllegalArgumentError',
      message: 'Cannot create a new partition reassignment without any replicas',
    });
  }
  // @ts-expect-error a null element the types forbid: Java's List.copyOf refuses it
  assert.throws(() => new NewPartitionReassignment([1, null]), { name: 'TypeError', message: 'targetReplicas must not contain null' });
  // @ts-expect-error a wrong argument: a string is no iterable of numbers, so it fits no form
  assert.throws(() => new NewPartitionReassignment('12'), {
    name: 'IllegalArgumentError',
    message: 'NewPartitionReassignment() takes one of (targetReplicas); got a string',
  });
  // @ts-expect-error a wrong argument: a string given by name
  assert.throws(() => new NewPartitionReassignment({ targetReplicas: '12' }), {
    name: 'TypeError',
    message: 'targetReplicas must be an iterable of numbers, not a string',
  });
});
