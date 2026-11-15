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

// TopicPartitionMap and TopicPartitionSet, the js-only collections keyed by
// TopicPartition (root CLAUDE.md, JavaScript Binding Conventions, Class family
// and Types, Q25). Java has no test of its own for them: they stand for Java's
// HashMap<TopicPartition, V> and HashSet<TopicPartition>, whose keys compare
// with TopicPartition.equals.

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { TopicPartition, TopicPartitionMap, TopicPartitionSet } = require('@confluentinc/kafka-javascript/common');

const tp = (topic, partition) => new TopicPartition({ topic, partition });

test('a TopicPartitionMap is keyed by TopicPartition.equals, keeping the original keys', () => {
  const first = tp('orders', 3);
  const map = new TopicPartitionMap([
    [first, 1n],
    [tp('orders', 4), 2n],
  ]);
  assert.equal(map.size(), 2);
  assert.equal(map.get(tp('orders', 3)), 1n);
  assert.equal(map.has(tp('orders', 4)), true);
  assert.equal(map.get(tp('orders', 5)), null);
  assert.equal(map.has({ key: tp('payments', 3) }), false);
  assert.equal([...map.keys()][0], first);
  assert.deepEqual([...map.values()], [1n, 2n]);
  assert.deepEqual([...map].map(([k, v]) => [k.toString(), v]), [['orders-3', 1n], ['orders-4', 2n]]);
  const seen = [];
  map.forEach((value, key, m) => seen.push([key.toString(), value, m === map]));
  assert.deepEqual(seen, [['orders-3', 1n, true], ['orders-4', 2n, true]]);
  assert.equal(map instanceof Map, false);
});

test('set keeps the original key and its place; delete and clear remove entries', () => {
  const first = tp('orders', 3);
  const map = new TopicPartitionMap();
  assert.equal(map.set(first, 'a'), map);
  map.set(tp('orders', 4), 'b');
  map.set(tp('orders', 3), 'c');
  assert.equal(map.size(), 2);
  assert.equal([...map.keys()][0], first);
  assert.deepEqual([...map.values()], ['c', 'b']);
  assert.equal(map.delete(tp('orders', 3)), true);
  assert.equal(map.delete({ key: tp('orders', 3) }), false);
  assert.equal(map.size(), 1);
  map.clear();
  assert.equal(map.size(), 0);
  assert.throws(() => map.set({ topic: 'orders', partition: 3 }, 'x'), { constructor: TypeError, message: 'key must be a TopicPartition or null' });
});

test('a null topic and the topic "null" are different keys, as Java keeps them apart', () => {
  const map = new TopicPartitionMap([
    [tp(null, 1), 'none'],
    [tp('null', 1), 'text'],
    [tp('a:1', 2), 'colon'],
    [tp('a', 1), 'plain'],
  ]);
  assert.equal(map.size(), 4);
  assert.equal(map.get(tp(null, 1)), 'none');
  assert.equal(map.get(tp('null', 1)), 'text');
  assert.equal(map.get(tp('a:1', 2)), 'colon');
});

test("toString is Java's AbstractMap.toString; toJSON keys by 'topic:partition' and writes a bigint as a decimal string", () => {
  const map = new TopicPartitionMap([
    [tp('orders', 0), 5n],
    [tp('orders', 1), 7n],
  ]);
  assert.equal(map.toString(), '{orders-0=5, orders-1=7}');
  assert.equal(String(new TopicPartitionMap()), '{}');
  assert.deepEqual(map.toJSON(), { 'orders:0': '5', 'orders:1': '7' });
  assert.equal(JSON.stringify({ m: map }), '{"m":{"orders:0":"5","orders:1":"7"}}');
  assert.deepEqual(new TopicPartitionMap([[tp('t', 0), { a: 1 }]]).toJSON(), { 't:0': { a: 1 } });
});

test("a TopicPartitionMap's arguments are checked", () => {
  const map = new TopicPartitionMap();
  assert.throws(() => map.get('x'), { constructor: IllegalArgumentError, message: 'get() takes one of (key); got a string' });
  assert.throws(() => map.has({ key: 'x' }), { constructor: TypeError, message: 'key must be a TopicPartition or null' });
  assert.throws(() => map.size(1), { constructor: TypeError, message: 'size() only reads: it takes no argument' });
  assert.throws(() => new TopicPartitionMap(undefined), { constructor: TypeError, message: "entries must not be undefined. Leave the argument out, or pass null for Java's null" });
  assert.throws(() => new TopicPartitionMap([[1, 2]]), { constructor: TypeError, message: 'an entry must be a [TopicPartition, value] pair' });
  assert.throws(() => new TopicPartitionMap(new Map([[tp('t', 0), 1]]), 2), { constructor: IllegalArgumentError });
  // Another map's entries, a TopicPartitionMap's included.
  const copy = new TopicPartitionMap(new TopicPartitionMap([[tp('t', 0), 1]]));
  assert.equal(copy.get(tp('t', 0)), 1);
});

test('a TopicPartitionSet compares its members by TopicPartition.equals, keeping the first', () => {
  const first = tp('orders', 3);
  const set = new TopicPartitionSet([first, tp('orders', 4), tp('orders', 3)]);
  assert.equal(set.size(), 2);
  assert.equal(set.has(tp('orders', 3)), true);
  assert.equal(set.has({ value: tp('orders', 5) }), false);
  assert.equal([...set][0], first);
  assert.equal(set.add(tp('orders', 3)), set);
  assert.equal(set.size(), 2);
  assert.equal([...set.values()][0], first);
  const seen = [];
  set.forEach((value, key, s) => seen.push([value.toString(), key === value, s === set]));
  assert.deepEqual(seen, [['orders-3', true, true], ['orders-4', true, true]]);
  assert.equal(set.delete(tp('orders', 3)), true);
  assert.equal(set.delete(tp('orders', 3)), false);
  set.clear();
  assert.equal(set.size(), 0);
  assert.equal(set instanceof Set, false);
});

test("a TopicPartitionSet's toString is Java's, its toJSON an array of { topic, partition }", () => {
  const set = new TopicPartitionSet([tp('orders', 0), tp('orders', 1)]);
  assert.equal(set.toString(), '[orders-0, orders-1]');
  assert.equal(JSON.stringify(set), '[{"topic":"orders","partition":0},{"topic":"orders","partition":1}]');
  assert.equal(String(new TopicPartitionSet()), '[]');
});

test("a TopicPartitionSet's arguments are checked", () => {
  const set = new TopicPartitionSet();
  assert.throws(() => set.add('x'), { constructor: IllegalArgumentError, message: 'add() takes one of (value); got a string' });
  assert.throws(() => set.add({ value: 'x' }), { constructor: TypeError, message: 'value must be a TopicPartition or null' });
  assert.throws(() => set.size(1), { constructor: TypeError, message: 'size() only reads: it takes no argument' });
  assert.throws(() => new TopicPartitionSet([1]), { constructor: TypeError, message: 'value must be a TopicPartition or null' });
  assert.throws(() => new TopicPartitionSet(5), { constructor: IllegalArgumentError });
});

test("a null key or member is held, as Java's HashMap and HashSet hold one, and prints as Java's null", () => {
  const map = new TopicPartitionMap([[null, 1n], [tp('null', 0), 2n]]);
  assert.equal(map.size(), 2);
  assert.equal(map.get(null), 1n);
  assert.equal(map.has({ key: null }), true);
  assert.equal(map.get(tp('null', 0)), 2n);
  assert.equal([...map.keys()][0], null);
  assert.equal(map.toString(), '{null=1, null-0=2}');
  assert.deepEqual(map.toJSON(), { null: '1', 'null:0': '2' });
  map.set(null, 3n);
  assert.equal(map.get(null), 3n);
  assert.equal(map.delete(null), true);
  assert.equal(map.has(null), false);
  const set = new TopicPartitionSet([null, tp('t', 0), null]);
  assert.equal(set.size(), 2);
  assert.equal(set.has(null), true);
  assert.equal([...set][0], null);
  assert.equal(set.toString(), '[null, t-0]');
  assert.equal(JSON.stringify(set), '[null,{"topic":"t","partition":0}]');
  assert.equal(set.add(null).size(), 2);
  assert.equal(set.delete(null), true);
});
