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

// NewPartitions: Java ships no test of it; the binding's checks of
// increaseTo's forms and its List<List<Integer>> taken as any iterable.

const assert = require('node:assert/strict');

const { NewPartitions } = require('@confluentinc/kafka-javascript/admin');

test('increaseTo takes the assignments as any iterable of iterables, copied', () => {
  const inner = [1, 2];
  const partitions = NewPartitions.increaseTo({ totalCount: 3, newAssignments: new Set([new Set([3]), inner]) });
  inner.push(9);
  assert.deepEqual(partitions.assignments(), [[3], [1, 2]]);
  assert.equal(partitions.totalCount(), 3);
  assert.equal(NewPartitions.increaseTo(4).assignments(), null);
  assert.equal(NewPartitions.increaseTo({ totalCount: 4, newAssignments: null }).assignments(), null);
  // @ts-expect-error Java's List<Integer>: a string is no list of broker ids
  assert.throws(() => NewPartitions.increaseTo({ totalCount: 3, newAssignments: ['12'] }), {
    name: 'TypeError',
    message: 'newAssignments must be an iterable of iterables of broker ids, not one holding a string',
  });
});

test('the JSON form', () => {
  assert.deepEqual(JSON.parse(JSON.stringify(NewPartitions.increaseTo({ totalCount: 3, newAssignments: [[1]] }))), {
    totalCount: 3,
    newAssignments: [[1]],
  });
});
