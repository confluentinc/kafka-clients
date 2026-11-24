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

// PartitionReassignment, a value class with Java's toString(). Java has no
// test of it.

const assert = require('node:assert/strict');

const { PartitionReassignment } = require('@confluentinc/kafka-javascript/admin');

test("PartitionReassignment: the three lists, copied, and Java's toString", () => {
  const r = new PartitionReassignment({ replicas: [0, 1, 2], addingReplicas: new Set([2]), removingReplicas: [] });
  assert.deepEqual(r.replicas(), [0, 1, 2]);
  assert.deepEqual(r.addingReplicas(), [2]);
  assert.deepEqual(r.removingReplicas(), []);
  assert.equal(r.toString(), 'PartitionReassignment(replicas=[0, 1, 2], addingReplicas=[2], removingReplicas=[])');
  assert.equal(JSON.stringify(r), '{"replicas":[0,1,2],"addingReplicas":[2],"removingReplicas":[]}');
  // @ts-expect-error a null the types forbid: Java's Collections.unmodifiableList dereferences it
  assert.throws(() => new PartitionReassignment({ replicas: null, addingReplicas: [], removingReplicas: [] }), {
    name: 'TypeError',
    message: 'replicas must not be null',
  });
  // Java's unmodifiableList keeps a null element, and its toString prints it.
  // @ts-expect-error a null element the types forbid: Java stores it
  const withNull = new PartitionReassignment({ replicas: [1, null], addingReplicas: [], removingReplicas: [] });
  assert.deepEqual(withNull.replicas(), [1, null]);
  assert.equal(withNull.toString(), 'PartitionReassignment(replicas=[1, null], addingReplicas=[], removingReplicas=[])');
  // @ts-expect-error a wrong element: a broker id is an integer
  assert.throws(() => new PartitionReassignment({ replicas: ['1'], addingReplicas: [], removingReplicas: [] }), {
    name: 'TypeError',
    message: 'an element of replicas must be an integer',
  });
  // @ts-expect-error an argument given to a reader: it only reads
  assert.throws(() => r.replicas(1), { name: 'TypeError', message: 'replicas() only reads: it takes no argument' });
});
