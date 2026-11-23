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

// Java's ResourceTypeTest.

const assert = require('node:assert/strict');

const { ResourceType } = require('@confluentinc/kafka-javascript/common/resource');

/** @type {Array<[import('@confluentinc/kafka-javascript/common/resource').ResourceType, number, string, boolean]>} */
const INFOS = [
  [ResourceType.UNKNOWN, 0, 'unknown', true],
  [ResourceType.ANY, 1, 'any', false],
  [ResourceType.TOPIC, 2, 'topic', false],
  [ResourceType.GROUP, 3, 'group', false],
  [ResourceType.CLUSTER, 4, 'cluster', false],
  [ResourceType.TRANSACTIONAL_ID, 5, 'transactional_id', false],
  [ResourceType.DELEGATION_TOKEN, 6, 'delegation_token', false],
  [ResourceType.USER, 7, 'user', false],
];

test('testIsUnknown', () => {
  for (const [resourceType, , , unknown] of INFOS) {
    assert.equal(resourceType.isUnknown(), unknown, `${resourceType} was supposed to have unknown == ${unknown}`);
  }
});

test('testCode', () => {
  assert.equal(ResourceType.values().length, INFOS.length);
  for (const [resourceType, code] of INFOS) {
    assert.equal(resourceType.code(), code, `${resourceType} was supposed to have code == ${code}`);
    assert.equal(ResourceType.fromCode(code), resourceType, `AclResourceType.fromCode(${code}) was supposed to be ${resourceType}`);
  }
  assert.equal(ResourceType.fromCode(120), ResourceType.UNKNOWN);
});

test('testName', () => {
  for (const [resourceType, , name] of INFOS) {
    assert.equal(ResourceType.fromString(name), resourceType, `ResourceType.fromString(${name}) was supposed to be ${resourceType}`);
  }
  assert.equal(ResourceType.fromString('something'), ResourceType.UNKNOWN);
});

test('testExhaustive', () => {
  assert.equal(INFOS.length, ResourceType.values().length);
  for (let i = 0; i < INFOS.length; i++) assert.equal(ResourceType.values()[i], INFOS[i][0]);
});

test('the enum members, the forms and the JSON form', () => {
  assert.equal(ResourceType.TOPIC.name(), 'TOPIC');
  assert.equal(ResourceType.TOPIC.ordinal(), 2);
  assert.equal(JSON.stringify(ResourceType.USER), '"USER"');
  assert.throws(() => ResourceType.valueOf('topic'), {
    name: 'IllegalArgumentError',
    message: 'No enum constant org.apache.kafka.common.resource.ResourceType.topic',
  });
  // @ts-expect-error a null string: Java dereferences it, a TypeError
  assert.throws(() => ResourceType.fromString(null), { name: 'TypeError', message: 'str must not be null' });
});
