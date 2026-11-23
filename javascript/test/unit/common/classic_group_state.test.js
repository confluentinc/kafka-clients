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

// ClassicGroupState, a Java enum whose toString() is its display name, with
// Java's parse. Java has no test of it: these check Java's members.

const assert = require('node:assert/strict');

const { ClassicGroupState } = require('@confluentinc/kafka-javascript/common');

test("ClassicGroupState: the constants in Java's order, parse case-insensitive", () => {
  assert.deepEqual(
    ClassicGroupState.values().map((s) => [s.name(), String(s)]),
    [
      ['UNKNOWN', 'Unknown'],
      ['PREPARING_REBALANCE', 'PreparingRebalance'],
      ['COMPLETING_REBALANCE', 'CompletingRebalance'],
      ['STABLE', 'Stable'],
      ['DEAD', 'Dead'],
      ['EMPTY', 'Empty'],
    ],
  );
  assert.equal(ClassicGroupState.parse('empty'), ClassicGroupState.EMPTY);
  assert.equal(ClassicGroupState.parse('Assigning'), ClassicGroupState.UNKNOWN);
  // @ts-expect-error a null name, which Java dereferences
  assert.throws(() => ClassicGroupState.parse(null), { name: 'TypeError', message: 'name must not be null' });
  assert.equal(JSON.stringify(ClassicGroupState.STABLE), '"STABLE"');
});
