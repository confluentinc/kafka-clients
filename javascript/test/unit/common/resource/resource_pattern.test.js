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

// Java's ResourcePatternTest (Java keeps it in its common.acl test package), then the
// class's equals, hashCode, toString, toFilter and JSON form.

const assert = require('node:assert/strict');

const { PatternType, ResourcePattern, ResourceType } = require('@confluentinc/kafka-javascript/common/resource');

test('shouldThrowIfResourceTypeIsAny', () => {
  assert.throws(() => new ResourcePattern({ resourceType: ResourceType.ANY, name: 'name', patternType: PatternType.LITERAL }), {
    name: 'IllegalArgumentError',
    message: 'resourceType must not be ANY',
  });
});

test('shouldThrowIfPatternTypeIsMatch', () => {
  assert.throws(() => new ResourcePattern({ resourceType: ResourceType.TOPIC, name: 'name', patternType: PatternType.MATCH }), {
    name: 'IllegalArgumentError',
    message: 'patternType must not be MATCH',
  });
});

test('shouldThrowIfPatternTypeIsAny', () => {
  assert.throws(() => new ResourcePattern({ resourceType: ResourceType.TOPIC, name: 'name', patternType: PatternType.ANY }), {
    name: 'IllegalArgumentError',
    message: 'patternType must not be ANY',
  });
});

test('shouldThrowIfResourceNameIsNull', () => {
  // Java's NullPointerException ("name"), a TypeError.
  // @ts-expect-error a null name
  assert.throws(() => new ResourcePattern({ resourceType: ResourceType.TOPIC, name: null, patternType: PatternType.ANY }), {
    name: 'TypeError',
    message: 'name must not be null',
  });
});

test("equals, hashCode, toString, toFilter and the JSON form are Java's", () => {
  const a = new ResourcePattern({ resourceType: ResourceType.TOPIC, name: 'orders', patternType: PatternType.PREFIXED });
  const b = new ResourcePattern({ resourceType: ResourceType.TOPIC, name: 'orders', patternType: PatternType.PREFIXED });
  const c = new ResourcePattern({ resourceType: ResourceType.GROUP, name: 'orders', patternType: PatternType.PREFIXED });
  assert.ok(a.equals(b));
  assert.ok(!a.equals(c));
  assert.ok(!a.equals(null));
  assert.equal(a.hashCode(), b.hashCode());
  assert.equal(a.toString(), 'ResourcePattern(resourceType=TOPIC, name=orders, patternType=PREFIXED)');
  assert.equal(JSON.stringify(a), '{"resourceType":"TOPIC","name":"orders","patternType":"PREFIXED"}');
  const filter = a.toFilter();
  assert.equal(filter.toString(), 'ResourcePattern(resourceType=TOPIC, name=orders, patternType=PREFIXED)');
  assert.ok(filter.matches(a));
  assert.equal(ResourcePattern.WILDCARD_RESOURCE, '*');
  assert.ok(new ResourcePattern({ resourceType: ResourceType.UNKNOWN, name: 'x', patternType: PatternType.LITERAL }).isUnknown());
  assert.ok(!a.isUnknown());
  // @ts-expect-error a field left out: Java's one constructor takes the three
  assert.throws(() => new ResourcePattern({ resourceType: ResourceType.TOPIC, name: 'x' }), {
    name: 'IllegalArgumentError',
    message: 'ResourcePattern() takes one of (resourceType, name, patternType); got (resourceType, name)',
  });
});
