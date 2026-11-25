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

// DescribeClassicGroupsResult through Java's public constructor. Java ships no
// test of the class.

const assert = require('node:assert/strict');

const { ClassicGroupDescription, DescribeClassicGroupsResult } = require('@confluentinc/kafka-javascript/admin');
const { ClassicGroupState, KafkaFuture } = require('@confluentinc/kafka-javascript/common');

test("DescribeClassicGroupsResult: Java's constructor stores a null map, which its methods dereference", async () => {
  // @ts-expect-error a null the types forbid: Java stores it
  const empty = new DescribeClassicGroupsResult(null);
  assert.throws(() => empty.describedGroups(), { name: 'TypeError', message: 'futures must not be null' });
  assert.throws(() => empty.all(), { name: 'TypeError', message: 'futures must not be null' });
  const description = new ClassicGroupDescription({
    groupId: 'g',
    protocol: '',
    protocolData: null,
    members: null,
    state: ClassicGroupState.EMPTY,
    coordinator: null,
  });
  const future = KafkaFuture.completedFuture({ value: description });
  const result = new DescribeClassicGroupsResult(new Map([['g', future]]));
  assert.deepEqual([...result.describedGroups().keys()], ['g']);
  assert.equal((await result.all().get()).get('g'), description);
});

test('DescribeClassicGroupsResult: futures is a Map or a plain object, never another object', () => {
  // A class instance is no map keyed by strings: it is refused, not read as an empty map.
  for (const [futures, name] of [
    [new Set(), 'a Set'],
    [new Date(0), 'a Date'],
  ]) {
    // @ts-expect-error a value the types forbid
    assert.throws(() => new DescribeClassicGroupsResult({ futures }), {
      name: 'TypeError',
      message: `futures must be a Map or a plain object, not ${name}`,
    });
  }
  assert.deepEqual([...new DescribeClassicGroupsResult({ futures: Object.create(null) }).describedGroups().keys()], []);
});
