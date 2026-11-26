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

// UuidMap, the binding's Map<Uuid, V> (js-only): keyed by Uuid.equals, as
// TopicPartitionMap is by TopicPartition.equals. Java has no test of it: it
// stands for the HashMap<Uuid, V> the admin client's Results hold
// (DeleteTopicsResult.topicIdValues(), DescribeTopicsResult.topicIdValues()).

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { Uuid, UuidMap } = require('@confluentinc/kafka-javascript/common');

/**
 * A Uuid of the two halves, built anew each call.
 *
 * @param {bigint} msb
 * @param {bigint} lsb
 */
const uuid = (msb, lsb) => new Uuid({ mostSigBits: msb, leastSigBits: lsb });

test('a UuidMap is keyed by Uuid.equals, keeping the original keys, negative halves included', () => {
  const first = uuid(-5n, -(2n ** 63n));
  const map = new UuidMap([
    [first, 1n],
    [uuid(1n, 2n), 2n],
  ]);
  assert.equal(map.size(), 2);
  assert.equal(map.get(uuid(-5n, -(2n ** 63n))), 1n);
  assert.equal(map.get(Uuid.fromString(first.toString())), 1n);
  assert.equal(map.has({ key: uuid(1n, 2n) }), true);
  assert.equal(map.get(uuid(1n, 3n)), null);
  assert.equal([...map.keys()][0], first);
  assert.deepEqual([...map.values()], [1n, 2n]);
  /** @type {Array<[string, bigint, boolean]>} */
  const seen = [];
  map.forEach((value, key, m) => seen.push([key.toString(), value, m === map]));
  assert.deepEqual(seen, [
    [first.toString(), 1n, true],
    [uuid(1n, 2n).toString(), 2n, true],
  ]);
  assert.equal(map instanceof Map, false);
});

test('set keeps the original key and its place; a repeated key is one entry; delete and clear remove entries', () => {
  const first = Uuid.ONE_UUID;
  const map = new UuidMap();
  assert.equal(map.set(first, 'a'), map);
  map.set(Uuid.ZERO_UUID, 'b');
  map.set(Uuid.fromString(Uuid.ONE_UUID.toString()), 'c');
  assert.equal(map.size(), 2);
  assert.equal([...map.keys()][0], first);
  assert.deepEqual([...map.values()], ['c', 'b']);
  assert.equal(map.delete(Uuid.fromString(Uuid.ONE_UUID.toString())), true);
  assert.equal(map.delete({ key: Uuid.ONE_UUID }), false);
  assert.equal(map.size(), 1);
  map.clear();
  assert.equal(map.size(), 0);
});

test("a null key is Java's null, apart from every Uuid; toString is Java's AbstractMap.toString and toJSON keys by the Uuid's text", () => {
  /** @type {UuidMap<bigint | string>} */
  const map = new UuidMap();
  map.set(Uuid.ONE_UUID, 1n).set(null, 'n');
  assert.equal(map.get(null), 'n');
  assert.equal(map.get(Uuid.ZERO_UUID), null);
  assert.equal(map.toString(), '{AAAAAAAAAAAAAAAAAAAAAQ=1, null=n}');
  assert.equal(String(new UuidMap()), '{}');
  assert.equal(JSON.stringify(map), '{"AAAAAAAAAAAAAAAAAAAAAQ":"1","null":"n"}');
});

test("a UuidMap's arguments are checked", () => {
  const map = new UuidMap();
  // @ts-expect-error a plain object for a Uuid: the key check throws TypeError
  assert.throws(() => map.set({}, 1), { constructor: TypeError, message: 'key must be a Uuid or null' });
  // @ts-expect-error no value: the undefined check throws TypeError
  assert.throws(() => map.set(Uuid.ONE_UUID), {
    constructor: TypeError,
    message: "value must not be undefined: pass null for Java's null",
  });
  // @ts-expect-error a bare string key: the form check throws IllegalArgumentError
  assert.throws(() => map.get('x'), { constructor: IllegalArgumentError, message: 'get() takes one of (key); got a string' });
  // @ts-expect-error an entry keyed by a number: the entry check throws TypeError
  assert.throws(() => new UuidMap([[1, 2]]), { constructor: TypeError, message: 'an entry must be a [Uuid, value] pair' });
  // @ts-expect-error an argument to a reader: the read-only check throws TypeError
  assert.throws(() => map.size(1), { constructor: TypeError, message: 'size() only reads: it takes no argument' });
  // Another map's entries, a UuidMap's included.
  assert.equal(new UuidMap(new UuidMap([[Uuid.ONE_UUID, 1]])).get(Uuid.ONE_UUID), 1);
});
