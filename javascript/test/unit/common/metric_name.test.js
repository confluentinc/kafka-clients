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

// MetricName and MetricNameMap. Java has no MetricNameTest; the hash codes are
// Java's, computed by Java's MetricName (kafka-clients) on the same values.

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { MetricName, MetricNameMap } = require('@confluentinc/kafka-javascript/common');

const tagged = (description = 'd') =>
  new MetricName({
    name: 'request-latency-avg',
    group: 'consumer-fetch-manager-metrics',
    description,
    tags: new Map([
      ['client-id', 'consumer-1'],
      ['a', 'b'],
    ]),
  });

test("Java's getters are methods; the tags are a new Map each call", () => {
  const name = tagged();
  assert.equal(name.name(), 'request-latency-avg');
  assert.equal(name.group(), 'consumer-fetch-manager-metrics');
  assert.equal(name.description(), 'd');
  assert.ok(name.tags() instanceof Map);
  assert.deepEqual([...name.tags()], [['client-id', 'consumer-1'], ['a', 'b']]);
  name.tags().set('x', 'y');
  assert.equal(name.tags().has('x'), false);
  assert.equal(name.toString(), 'MetricName [name=request-latency-avg, group=consumer-fetch-manager-metrics, description=d, tags={client-id=consumer-1, a=b}]');
});

test("equals and hashCode are Java's: group, name and tags, not the description", () => {
  // Java: new MetricName("name", "group", "description", Map.of()).hashCode().
  assert.equal(new MetricName({ name: 'name', group: 'group', description: 'description', tags: new Map() }).hashCode(), 398040563);
  assert.equal(tagged().hashCode(), -568034115);
  assert.equal(tagged('other').hashCode(), tagged().hashCode());
  assert.equal(tagged().equals(tagged('other')), true);
  const reordered = new MetricName({
    name: 'request-latency-avg',
    group: 'consumer-fetch-manager-metrics',
    description: 'd',
    tags: new Map([
      ['a', 'b'],
      ['client-id', 'consumer-1'],
    ]),
  });
  assert.equal(tagged().equals({ obj: reordered }), true);
  assert.equal(reordered.hashCode(), tagged().hashCode());
  assert.equal(tagged().equals(new MetricName({ name: 'request-latency-avg', group: 'g', description: 'd', tags: new Map() })), false);
  assert.equal(tagged().equals(null), false);
});

test('the tags are a Map or a plain object, copied when the name is built', () => {
  const given = new Map([['client-id', 'c']]);
  const fromMap = new MetricName({ name: 'n', group: 'g', description: '', tags: given });
  const fromObject = new MetricName({ name: 'n', group: 'g', description: '', tags: { 'client-id': 'c' } });
  assert.deepEqual([...fromObject.tags()], [['client-id', 'c']]);
  assert.equal(fromMap.equals(fromObject), true);
  assert.equal(fromMap.hashCode(), fromObject.hashCode());
  given.set('other', 'x');
  assert.deepEqual([...fromMap.tags()], [['client-id', 'c']]);
  assert.deepEqual([...new MetricName({ name: 'n', group: 'g', description: '', tags: {} }).tags()], []);
});

test("the constructor checks Java's requireNonNull and the run-time types", () => {
  for (const field of ['name', 'group', 'description', 'tags']) {
    const options = { name: 'n', group: 'g', description: 'd', tags: new Map(), [field]: null };
    assert.throws(() => new MetricName(options), { constructor: TypeError, message: `${field} must not be null` });
  }
  for (const tags of [[['a', 'b']], new Map([['a', 1]]), { a: 1 }, 'a=b']) {
    assert.throws(() => new MetricName({ name: 'n', group: 'g', description: 'd', tags }), {
      constructor: TypeError,
      message: 'tags must be a Map or a plain object of string to string',
    });
  }
  assert.throws(() => new MetricName({ name: 'n', group: 'g' }), {
    constructor: IllegalArgumentError,
    message: 'MetricName() takes one of (name, group, description, tags); got (name, group)',
  });
});

test('a MetricNameMap is keyed by MetricName.equals, keeping the original keys', () => {
  const first = tagged('first');
  const map = new MetricNameMap([
    [first, 1],
    [new MetricName({ name: 'x', group: 'g', description: '', tags: new Map() }), 2],
  ]);
  assert.equal(map.size(), 2);
  assert.equal(map.get(tagged('another description')), 1);
  assert.equal(map.has(tagged()), true);
  assert.equal(map.get(new MetricName({ name: 'y', group: 'g', description: '', tags: new Map() })), undefined);
  assert.equal([...map.keys()][0], first);
  assert.deepEqual([...map.values()], [1, 2]);
  assert.deepEqual([...map].map(([k, v]) => [k.name(), v]), [['request-latency-avg', 1], ['x', 2]]);
  const seen = [];
  map.forEach((value, key, m) => seen.push([key.name(), value, m === map]));
  assert.deepEqual(seen, [['request-latency-avg', 1, true], ['x', 2, true]]);
  assert.equal(map instanceof Map, false);
  // toJSON: the entries by the map's string key, the group, the name and the sorted tags.
  assert.deepEqual(JSON.parse(JSON.stringify(map)), {
    '["consumer-fetch-manager-metrics","request-latency-avg",[["a","b"],["client-id","consumer-1"]]]': 1,
    '["g","x",[]]': 2,
  });
  assert.deepEqual(new MetricNameMap().toJSON(), {});
  assert.equal(new MetricNameMap().size(), 0);
  assert.throws(() => map.get('x'), { constructor: IllegalArgumentError, message: 'get() takes one of (key); got a string' });
  assert.throws(() => map.has({ key: 'x' }), { constructor: TypeError, message: 'key must be a MetricName' });
  assert.throws(() => new MetricNameMap([[1, 2]]), { constructor: TypeError, message: 'an entry must be a [MetricName, value] pair' });
});
