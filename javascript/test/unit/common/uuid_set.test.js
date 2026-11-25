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

// UuidSet, the binding's Set<Uuid> (js-only): its members compare as
// Uuid.equals does, as TopicPartitionSet's do. Java has no test of it.

const assert = require('node:assert/strict');

const { Uuid, UuidSet } = require('@confluentinc/kafka-javascript/common');

test('members compare by value: an equal Uuid is the same member, the first one kept', () => {
  const first = new Uuid({ mostSigBits: 1n, leastSigBits: 2n });
  const set = new UuidSet([first, new Uuid({ mostSigBits: 1n, leastSigBits: 2n }), Uuid.ONE_UUID]);
  assert.equal(set.size(), 2);
  assert.equal(set.has(new Uuid({ mostSigBits: 1n, leastSigBits: 2n })), true);
  assert.equal(set.has({ value: Uuid.fromString(Uuid.ONE_UUID.toString()) }), true);
  assert.equal(set.has(Uuid.ZERO_UUID), false);
  assert.equal([...set][0], first);
  assert.equal(set.add(new Uuid({ mostSigBits: 1n, leastSigBits: 2n })), set);
  assert.equal(set.size(), 2);
  assert.equal(set.delete({ value: new Uuid({ mostSigBits: 1n, leastSigBits: 2n }) }), true);
  assert.equal(set.delete(new Uuid({ mostSigBits: 1n, leastSigBits: 2n })), false);
  assert.deepEqual([...set.values()], [Uuid.ONE_UUID]);
  set.clear();
  assert.equal(set.size(), 0);
});

test('the forms: no argument, the values on their own or by name; a null member is held, as a HashSet holds one', () => {
  assert.equal(new UuidSet().size(), 0);
  const set = new UuidSet({ values: [Uuid.ZERO_UUID, null] });
  assert.equal(set.has(null), true);
  assert.equal(set.toString(), '[AAAAAAAAAAAAAAAAAAAAAA, null]');
  assert.equal(JSON.stringify(set), '["AAAAAAAAAAAAAAAAAAAAAA",null]');
  /** @type {Array<[unknown, unknown]>} */
  const seen = [];
  set.forEach((value, key) => void seen.push([value, key]));
  assert.deepEqual(seen, [
    [Uuid.ZERO_UUID, Uuid.ZERO_UUID],
    [null, null],
  ]);
  // @ts-expect-error a wrong member: a set of Uuid
  assert.throws(() => new UuidSet(['AAAAAAAAAAAAAAAAAAAAAA']), { name: 'TypeError', message: 'value must be a Uuid or null' });
  // @ts-expect-error a wrong member: a set of Uuid
  assert.throws(() => set.has('AAAAAAAAAAAAAAAAAAAAAA'), {
    name: 'IllegalArgumentError',
    message: 'has() takes one of (value); got a string',
  });
  assert.equal(set instanceof Set, false);
});
