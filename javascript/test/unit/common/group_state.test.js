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

// GroupState, a Java enum whose toString() is its display name, with Java's
// parse and groupStatesForType. Java has no test of it: these check Java's
// members, as python/confluent_kafka/common/group_state.py.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { GroupState, GroupType } = require('@confluentinc/kafka-javascript/common');

test("GroupState: the constants in Java's order, toString the display name, toJSON the name", () => {
  assert.deepEqual(
    GroupState.values().map((s) => [s.name(), s.ordinal(), String(s), JSON.stringify(s)]),
    [
      ['UNKNOWN', 0, 'Unknown', '"UNKNOWN"'],
      ['PREPARING_REBALANCE', 1, 'PreparingRebalance', '"PREPARING_REBALANCE"'],
      ['COMPLETING_REBALANCE', 2, 'CompletingRebalance', '"COMPLETING_REBALANCE"'],
      ['STABLE', 3, 'Stable', '"STABLE"'],
      ['DEAD', 4, 'Dead', '"DEAD"'],
      ['EMPTY', 5, 'Empty', '"EMPTY"'],
      ['ASSIGNING', 6, 'Assigning', '"ASSIGNING"'],
      ['RECONCILING', 7, 'Reconciling', '"RECONCILING"'],
      ['NOT_READY', 8, 'NotReady', '"NOT_READY"'],
    ],
  );
  assert.ok(Object.isFrozen(GroupState.STABLE));
});

test('GroupState.parse: case-insensitive over the display names, UNKNOWN otherwise; null is a TypeError', () => {
  assert.equal(GroupState.parse('stable'), GroupState.STABLE);
  assert.equal(GroupState.parse({ name: 'PREPARINGREBALANCE' }), GroupState.PREPARING_REBALANCE);
  assert.equal(GroupState.parse('NotReady'), GroupState.NOT_READY);
  assert.equal(GroupState.parse('PREPARING_REBALANCE'), GroupState.UNKNOWN);
  assert.equal(GroupState.parse('bogus'), GroupState.UNKNOWN);
  // @ts-expect-error a null name, which Java dereferences
  assert.throws(() => GroupState.parse(null), { name: 'TypeError', message: 'name must not be null' });
});

test("GroupState.valueOf: exact names, with Enum.valueOf's errors", () => {
  assert.equal(GroupState.valueOf('DEAD'), GroupState.DEAD);
  assert.throws(() => GroupState.valueOf('Dead'), {
    name: 'IllegalArgumentError',
    message: 'No enum constant org.apache.kafka.common.GroupState.Dead',
  });
  // @ts-expect-error a null name, which Enum.valueOf refuses
  assert.throws(() => GroupState.valueOf(null), { name: 'TypeError', message: 'Name is null' });
});

test("GroupState.groupStatesForType: Java's sets, and UNKNOWN refused", () => {
  const G = GroupState;
  assert.deepEqual(
    GroupState.groupStatesForType(GroupType.CLASSIC),
    new Set([G.PREPARING_REBALANCE, G.COMPLETING_REBALANCE, G.STABLE, G.DEAD, G.EMPTY]),
  );
  assert.deepEqual(
    GroupState.groupStatesForType({ type: GroupType.CONSUMER }),
    new Set([G.PREPARING_REBALANCE, G.COMPLETING_REBALANCE, G.STABLE, G.DEAD, G.EMPTY, G.ASSIGNING, G.RECONCILING]),
  );
  assert.deepEqual(
    GroupState.groupStatesForType(GroupType.STREAMS),
    new Set([G.STABLE, G.DEAD, G.EMPTY, G.ASSIGNING, G.RECONCILING, G.NOT_READY]),
  );
  assert.deepEqual(GroupState.groupStatesForType(GroupType.SHARE), new Set([G.STABLE, G.DEAD, G.EMPTY]));
  assert.throws(
    () => GroupState.groupStatesForType(GroupType.UNKNOWN),
    (e) => e instanceof IllegalArgumentError && e.message === 'Group type not known',
  );
});
