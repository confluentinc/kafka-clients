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

// Java's AclBindingTest, then the members of AclBinding, AclBindingFilter,
// AccessControlEntry and AccessControlEntryFilter it does not cover, and the
// maps keyed by them (js-only).

const assert = require('node:assert/strict');

const {
  AccessControlEntry,
  AccessControlEntryFilter,
  AclBinding,
  AclBindingFilter,
  AclBindingFilterMap,
  AclBindingMap,
  AclOperation,
  AclPermissionType,
} = require('@confluentinc/kafka-javascript/common/acl');
const { PatternType, ResourcePattern, ResourcePatternFilter, ResourceType } = require('@confluentinc/kafka-javascript/common/resource');

/**
 * @param {string} name
 * @param {import('@confluentinc/kafka-javascript/common/resource').PatternType} [patternType]
 * @param {import('@confluentinc/kafka-javascript/common/resource').ResourceType} [resourceType]
 */
const pattern = (name, patternType = PatternType.LITERAL, resourceType = ResourceType.TOPIC) =>
  new ResourcePattern({ resourceType, name, patternType });
/**
 * @param {string} principal
 * @param {string} host
 * @param {import('@confluentinc/kafka-javascript/common/acl').AclOperation} operation
 * @param {import('@confluentinc/kafka-javascript/common/acl').AclPermissionType} permissionType
 */
const entry = (principal, host, operation, permissionType) => new AccessControlEntry({ principal, host, operation, permissionType });

const ACL1 = new AclBinding({ pattern: pattern('mytopic'), entry: entry('User:ANONYMOUS', '', AclOperation.ALL, AclPermissionType.ALLOW) });
const ACL2 = new AclBinding({ pattern: pattern('mytopic'), entry: entry('User:*', '', AclOperation.READ, AclPermissionType.ALLOW) });
const ACL3 = new AclBinding({
  pattern: pattern('mytopic2'),
  entry: entry('User:ANONYMOUS', '127.0.0.1', AclOperation.READ, AclPermissionType.DENY),
});
const UNKNOWN_ACL = new AclBinding({
  pattern: pattern('mytopic2'),
  entry: entry('User:ANONYMOUS', '127.0.0.1', AclOperation.UNKNOWN, AclPermissionType.DENY),
});
const ANY_ANONYMOUS = new AclBindingFilter({
  patternFilter: ResourcePatternFilter.ANY,
  entryFilter: new AccessControlEntryFilter({
    principal: 'User:ANONYMOUS',
    host: null,
    operation: AclOperation.ANY,
    permissionType: AclPermissionType.ANY,
  }),
});
const ANY_DENY = new AclBindingFilter({
  patternFilter: ResourcePatternFilter.ANY,
  entryFilter: new AccessControlEntryFilter({
    principal: null,
    host: null,
    operation: AclOperation.ANY,
    permissionType: AclPermissionType.DENY,
  }),
});
const ANY_MYTOPIC = new AclBindingFilter({
  patternFilter: new ResourcePatternFilter({ resourceType: ResourceType.TOPIC, name: 'mytopic', patternType: PatternType.LITERAL }),
  entryFilter: new AccessControlEntryFilter({
    principal: null,
    host: null,
    operation: AclOperation.ANY,
    permissionType: AclPermissionType.ANY,
  }),
});

test('testMatching', () => {
  assert.ok(ACL1.equals(ACL1));
  const acl1Copy = new AclBinding({
    pattern: pattern('mytopic'),
    entry: entry('User:ANONYMOUS', '', AclOperation.ALL, AclPermissionType.ALLOW),
  });
  assert.ok(ACL1.equals(acl1Copy));
  assert.ok(acl1Copy.equals(ACL1));
  assert.ok(ACL2.equals(ACL2));
  assert.ok(!ACL1.equals(ACL2));
  assert.ok(!ACL2.equals(ACL1));
  assert.ok(AclBindingFilter.ANY.matches(ACL1));
  assert.ok(!AclBindingFilter.ANY.equals(ACL1));
  assert.ok(AclBindingFilter.ANY.matches(ACL2));
  assert.ok(!AclBindingFilter.ANY.equals(ACL2));
  assert.ok(AclBindingFilter.ANY.matches(ACL3));
  assert.ok(!AclBindingFilter.ANY.equals(ACL3));
  assert.ok(AclBindingFilter.ANY.equals(AclBindingFilter.ANY));
  assert.ok(ANY_ANONYMOUS.matches(ACL1));
  assert.ok(!ANY_ANONYMOUS.equals(ACL1));
  assert.ok(!ANY_ANONYMOUS.matches(ACL2));
  assert.ok(!ANY_ANONYMOUS.equals(ACL2));
  assert.ok(ANY_ANONYMOUS.matches(ACL3));
  assert.ok(!ANY_ANONYMOUS.equals(ACL3));
  assert.ok(!ANY_DENY.matches(ACL1));
  assert.ok(!ANY_DENY.matches(ACL2));
  assert.ok(ANY_DENY.matches(ACL3));
  assert.ok(ANY_MYTOPIC.matches(ACL1));
  assert.ok(ANY_MYTOPIC.matches(ACL2));
  assert.ok(!ANY_MYTOPIC.matches(ACL3));
  assert.ok(ANY_ANONYMOUS.matches(UNKNOWN_ACL));
  assert.ok(ANY_DENY.matches(UNKNOWN_ACL));
  assert.ok(UNKNOWN_ACL.equals(UNKNOWN_ACL));
  assert.ok(!ANY_MYTOPIC.matches(UNKNOWN_ACL));
});

test('testUnknowns', () => {
  assert.ok(!ACL1.isUnknown());
  assert.ok(!ACL2.isUnknown());
  assert.ok(!ACL3.isUnknown());
  assert.ok(!ANY_ANONYMOUS.isUnknown());
  assert.ok(!ANY_DENY.isUnknown());
  assert.ok(!ANY_MYTOPIC.isUnknown());
  assert.ok(UNKNOWN_ACL.isUnknown());
});

test('testMatchesAtMostOne', () => {
  assert.equal(ACL1.toFilter().findIndefiniteField(), null);
  assert.equal(ACL2.toFilter().findIndefiniteField(), null);
  assert.equal(ACL3.toFilter().findIndefiniteField(), null);
  assert.ok(!ANY_ANONYMOUS.matchesAtMostOne());
  assert.ok(!ANY_DENY.matchesAtMostOne());
  assert.ok(!ANY_MYTOPIC.matchesAtMostOne());
});

test('shouldNotThrowOnUnknownPatternType', () => {
  new AclBinding({ pattern: pattern('foo', PatternType.UNKNOWN), entry: ACL1.entry() });
});

test('shouldNotThrowOnUnknownResourceType', () => {
  new AclBinding({ pattern: pattern('foo', PatternType.LITERAL, ResourceType.UNKNOWN), entry: ACL1.entry() });
});

test('shouldThrowOnMatchPatternType', () => {
  assert.throws(() => new AclBinding({ pattern: pattern('foo', PatternType.MATCH), entry: ACL1.entry() }), {
    name: 'IllegalArgumentError',
    message: 'patternType must not be MATCH',
  });
});

test('shouldThrowOnAnyPatternType', () => {
  assert.throws(() => new AclBinding({ pattern: pattern('foo', PatternType.ANY), entry: ACL1.entry() }), {
    name: 'IllegalArgumentError',
    message: 'patternType must not be ANY',
  });
});

test('shouldThrowOnAnyResourceType', () => {
  assert.throws(() => new AclBinding({ pattern: pattern('foo', PatternType.LITERAL, ResourceType.ANY), entry: ACL1.entry() }), {
    name: 'IllegalArgumentError',
    message: 'resourceType must not be ANY',
  });
});

test("toString, hashCode and the JSON form are Java's, and equal values hash alike", () => {
  assert.equal(
    ACL3.toString(),
    '(pattern=ResourcePattern(resourceType=TOPIC, name=mytopic2, patternType=LITERAL), ' +
      'entry=(principal=User:ANONYMOUS, host=127.0.0.1, operation=READ, permissionType=DENY))',
  );
  assert.equal(
    AclBindingFilter.ANY.toString(),
    '(patternFilter=ResourcePattern(resourceType=ANY, name=<any>, patternType=ANY), ' +
      'entryFilter=(principal=<any>, host=<any>, operation=ANY, permissionType=ANY))',
  );
  assert.equal(
    JSON.stringify(ACL3),
    '{"pattern":{"resourceType":"TOPIC","name":"mytopic2","patternType":"LITERAL"},' +
      '"entry":{"principal":"User:ANONYMOUS","host":"127.0.0.1","operation":"READ","permissionType":"DENY"}}',
  );
  assert.equal(
    JSON.stringify(ANY_DENY),
    '{"patternFilter":{"resourceType":"ANY","name":null,"patternType":"ANY"},' +
      '"entryFilter":{"principal":null,"host":null,"operation":"ANY","permissionType":"DENY"}}',
  );
  const copy = new AclBinding({
    pattern: pattern('mytopic2'),
    entry: entry('User:ANONYMOUS', '127.0.0.1', AclOperation.READ, AclPermissionType.DENY),
  });
  assert.equal(copy.hashCode(), ACL3.hashCode());
  assert.ok(ACL3.toFilter().equals(copy.toFilter()));
  assert.equal(ACL3.toFilter().hashCode(), copy.toFilter().hashCode());
  assert.ok(ACL3.toFilter().matches(ACL3));
  assert.ok(ACL3.entry().toFilter().matches(ACL3.entry()));
});

test("the constructors' null checks and ANY checks, in Java's order", () => {
  // @ts-expect-error a null pattern
  assert.throws(() => new AclBinding({ pattern: null, entry: ACL1.entry() }), { name: 'TypeError', message: 'pattern must not be null' });
  // @ts-expect-error a null entry
  assert.throws(() => new AclBinding({ pattern: ACL1.pattern(), entry: null }), { name: 'TypeError', message: 'entry must not be null' });
  assert.throws(() => entry('User:a', '*', AclOperation.ANY, AclPermissionType.ALLOW), {
    name: 'IllegalArgumentError',
    message: 'operation must not be ANY',
  });
  // Java checks the operation's ANY before the permission type's null.
  // @ts-expect-error a null permission type
  assert.throws(() => entry('User:a', '*', AclOperation.ANY, null), { name: 'IllegalArgumentError', message: 'operation must not be ANY' });
  assert.throws(() => entry('User:a', '*', AclOperation.READ, AclPermissionType.ANY), {
    name: 'IllegalArgumentError',
    message: 'permissionType must not be ANY',
  });
  // @ts-expect-error a null principal
  assert.throws(() => entry(null, '*', AclOperation.READ, AclPermissionType.ALLOW), {
    name: 'TypeError',
    message: 'principal must not be null',
  });
  // @ts-expect-error a null host
  assert.throws(() => entry('User:a', null, AclOperation.READ, AclPermissionType.ALLOW), {
    name: 'TypeError',
    message: 'host must not be null',
  });
  assert.throws(
    // @ts-expect-error a null operation
    () => new AccessControlEntryFilter({ principal: null, host: null, operation: null, permissionType: AclPermissionType.ANY }),
    { name: 'TypeError', message: 'operation must not be null' },
  );
  // @ts-expect-error a null pattern filter
  assert.throws(() => new AclBindingFilter({ patternFilter: null, entryFilter: AccessControlEntryFilter.ANY }), {
    name: 'TypeError',
    message: 'patternFilter must not be null',
  });
  // @ts-expect-error a null binding
  assert.throws(() => AclBindingFilter.ANY.matches(null), { name: 'TypeError', message: 'binding must not be null' });
});

test('AccessControlEntryFilter reads the entry only where Java dereferences it, and names its indefinite fields', () => {
  // @ts-expect-error a null entry: ANY reads none of its fields
  assert.equal(AccessControlEntryFilter.ANY.matches(null), true);
  // @ts-expect-error a null entry, which this filter reads
  assert.throws(() => ANY_ANONYMOUS.entryFilter().matches(null), { name: 'TypeError', message: 'other must not be null' });
  const f = (/** @type {string | null} */ p, /** @type {string | null} */ h, /** @type {any} */ op, /** @type {any} */ perm) =>
    new AccessControlEntryFilter({ principal: p, host: h, operation: op, permissionType: perm }).findIndefiniteField();
  assert.equal(f(null, '*', AclOperation.READ, AclPermissionType.ALLOW), 'Principal is NULL');
  assert.equal(f('u', null, AclOperation.READ, AclPermissionType.ALLOW), 'Host is NULL');
  assert.equal(f('u', '*', AclOperation.ANY, AclPermissionType.ALLOW), 'Operation is ANY');
  assert.equal(f('u', '*', AclOperation.UNKNOWN, AclPermissionType.ALLOW), 'Operation is UNKNOWN');
  assert.equal(f('u', '*', AclOperation.READ, AclPermissionType.ANY), 'Permission type is ANY');
  assert.equal(f('u', '*', AclOperation.READ, AclPermissionType.UNKNOWN), 'Permission type is UNKNOWN');
  assert.equal(f('u', '*', AclOperation.READ, AclPermissionType.ALLOW), null);
  assert.equal(ANY_MYTOPIC.findIndefiniteField(), 'Principal is NULL');
  assert.equal(AclBindingFilter.ANY.findIndefiniteField(), 'Resource type is ANY.');
});

test('the maps keyed by AclBinding and AclBindingFilter compare their keys as equals does, keeping the original keys', () => {
  const copy = new AclBinding({
    pattern: pattern('mytopic'),
    entry: entry('User:ANONYMOUS', '', AclOperation.ALL, AclPermissionType.ALLOW),
  });
  const map = new AclBindingMap([
    [ACL1, 1],
    [ACL2, 2],
  ]);
  assert.equal(map.size(), 2);
  assert.equal(map.get(copy), 1);
  assert.equal(map.get(ACL3), null);
  map.set(copy, 3);
  assert.equal(map.size(), 2);
  assert.equal([...map.keys()][0], ACL1);
  assert.equal(map.get({ key: ACL1 }), 3);
  assert.equal(map instanceof Map, false);
  assert.equal(map.toString(), `{${ACL1}=3, ${ACL2}=2}`);
  const filters = new AclBindingFilterMap([[AclBindingFilter.ANY, 'any']]);
  const anyCopy = new AclBindingFilter({ patternFilter: ResourcePatternFilter.ANY, entryFilter: AccessControlEntryFilter.ANY });
  assert.equal(filters.get(anyCopy), 'any');
  assert.equal(filters.has(ANY_DENY), false);
  assert.equal(JSON.stringify(filters), '{"[\\"ANY\\",null,\\"ANY\\",null,null,\\"ANY\\",\\"ANY\\"]":"any"}');
  // @ts-expect-error a key of another class: an AclBindingMap is keyed by AclBinding
  assert.throws(() => map.get(AclBindingFilter.ANY), {
    name: 'IllegalArgumentError',
    message: 'get() takes one of (key); got an AclBindingFilter',
  });
});
