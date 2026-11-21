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

// Java's ConfigResourceTest, and the binding's own tests of ConfigResource
// (Java's equals, hashCode and toString, the forms, toJSON) and of
// ConfigResourceMap, the js-only map keyed by ConfigResource (root
// CLAUDE.md, JavaScript Binding Conventions, Class family, Q25; design D2),
// which Java has no test of: it stands for Java's HashMap<ConfigResource, V>.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { ConfigResource, ConfigResourceMap } = require('@confluentinc/kafka-javascript/common/config');

const { Type } = ConfigResource;

test('shouldGetTypeFromId', () => {
  assert.equal(Type.forId(2), Type.TOPIC);
  assert.equal(Type.forId(4), Type.BROKER);
  assert.equal(Type.forId(16), Type.CLIENT_METRICS);
  assert.equal(Type.forId(32), Type.GROUP);
});

test('shouldReturnUnknownForUnknownCode', () => {
  assert.equal(Type.forId(-1), Type.UNKNOWN);
  assert.equal(Type.forId(0), Type.UNKNOWN);
  assert.equal(Type.forId(1), Type.UNKNOWN);
});

test('shouldRoundTripEveryType', () => {
  for (const type of Type.values()) assert.equal(Type.forId(type.id()), type, type.toString());
});

test("Type is Java's enum: its constants in order, valueOf, forId's byte range", () => {
  assert.deepEqual(
    Type.values().map((t) => [t.name(), t.ordinal(), t.id()]),
    [
      ['GROUP', 0, 32],
      ['CLIENT_METRICS', 1, 16],
      ['BROKER_LOGGER', 2, 8],
      ['BROKER', 3, 4],
      ['TOPIC', 4, 2],
      ['UNKNOWN', 5, 0],
    ],
  );
  assert.equal(Type.valueOf('TOPIC'), Type.TOPIC);
  assert.equal(Type.valueOf({ name: 'BROKER' }), Type.BROKER);
  assert.throws(() => Type.valueOf('topic'), {
    name: 'IllegalArgumentError',
    message: 'No enum constant org.apache.kafka.common.config.ConfigResource.Type.topic',
  });
  // @ts-expect-error a null name, which the types forbid: Enum.valueOf's NullPointerException
  assert.throws(() => Type.valueOf(null), { name: 'TypeError', message: 'Name is null' });
  assert.equal(Type.forId({ id: 8 }), Type.BROKER_LOGGER);
  assert.throws(() => Type.forId(128), { name: 'RangeError', message: 'id must be from -128 to 127' });
  assert.throws(() => Type.forId(1.5), { name: 'TypeError', message: 'id must be an integer' });
  assert.equal(String(Type.TOPIC), 'TOPIC');
  assert.equal(JSON.stringify({ t: Type.TOPIC }), '{"t":"TOPIC"}');
  assert.ok(Object.isFrozen(Type.TOPIC));
});

test("ConfigResource keeps Java's null checks, equals, hashCode, toString and isDefault", () => {
  const topic = new ConfigResource({ type: Type.TOPIC, name: 'orders' });
  assert.equal(topic.type(), Type.TOPIC);
  assert.equal(topic.name(), 'orders');
  assert.equal(topic.isDefault(), false);
  assert.equal(new ConfigResource({ type: Type.BROKER, name: '' }).isDefault(), true);
  assert.equal(topic.toString(), "ConfigResource(type=TOPIC, name='orders')");
  const same = new ConfigResource({ type: Type.TOPIC, name: 'orders' });
  assert.equal(topic.equals(same), true);
  assert.equal(topic.equals({ o: same }), true);
  assert.equal(topic.hashCode(), same.hashCode());
  assert.equal(topic.equals(new ConfigResource({ type: Type.BROKER, name: 'orders' })), false);
  assert.equal(topic.equals(new ConfigResource({ type: Type.TOPIC, name: 'payments' })), false);
  assert.equal(topic.equals(null), false);
  assert.equal(JSON.stringify(topic), '{"type":"TOPIC","name":"orders"}');
  // @ts-expect-error a null type, which the types forbid: Java's Objects.requireNonNull
  assert.throws(() => new ConfigResource({ type: null, name: 'x' }), { name: 'TypeError', message: 'type should not be null' });
  // @ts-expect-error a null name, which the types forbid: Java's Objects.requireNonNull
  assert.throws(() => new ConfigResource({ type: Type.TOPIC, name: null }), { name: 'TypeError', message: 'name should not be null' });
  // @ts-expect-error a key no form has: the form check
  assert.throws(() => new ConfigResource({ type: Type.TOPIC }), IllegalArgumentError);
  // @ts-expect-error an argument given to a reader: it only reads
  assert.throws(() => topic.name(1), { name: 'TypeError', message: 'name() only reads: it takes no argument' });
});

test('a ConfigResourceMap is keyed by ConfigResource.equals, keeping the original keys', () => {
  const first = new ConfigResource({ type: Type.TOPIC, name: 'orders' });
  const map = new ConfigResourceMap([
    [first, 1],
    [new ConfigResource({ type: Type.BROKER, name: 'orders' }), 2],
  ]);
  assert.equal(map.size(), 2);
  assert.equal(map.get(new ConfigResource({ type: Type.TOPIC, name: 'orders' })), 1);
  assert.equal(map.get({ key: new ConfigResource({ type: Type.BROKER, name: 'orders' }) }), 2);
  assert.equal(map.get(new ConfigResource({ type: Type.GROUP, name: 'orders' })), null);
  assert.equal([...map.keys()][0], first);
  map.set(new ConfigResource({ type: Type.TOPIC, name: 'orders' }), 3);
  assert.equal(map.size(), 2);
  assert.equal([...map.keys()][0], first);
  assert.equal(map.get(first), 3);
  assert.equal(map.toString(), "{ConfigResource(type=TOPIC, name='orders')=3, ConfigResource(type=BROKER, name='orders')=2}");
  assert.equal(JSON.stringify(map), '{"[\\"TOPIC\\",\\"orders\\"]":3,"[\\"BROKER\\",\\"orders\\"]":2}');
  assert.equal(map.delete(first), true);
  assert.equal(map.has(first), false);
  map.set(null, 4);
  assert.equal(map.get(null), 4);
  map.clear();
  assert.equal(map.size(), 0);
  assert.equal(map instanceof Map, false);
  // @ts-expect-error a key of another class: a ConfigResourceMap is keyed by ConfigResource
  assert.throws(() => map.get('orders'), { name: 'IllegalArgumentError' });
  // @ts-expect-error an undefined value: null is Java's null
  assert.throws(() => map.set(first, undefined), { name: 'TypeError', message: "value must not be undefined: pass null for Java's null" });
});
