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

// Java's AclPermissionTypeTest.

const assert = require('node:assert/strict');

const { AclPermissionType } = require('@confluentinc/kafka-javascript/common/acl');

/** @type {Array<[import('@confluentinc/kafka-javascript/common/acl').AclPermissionType, number, string, boolean]>} */
const INFOS = [
  [AclPermissionType.UNKNOWN, 0, 'unknown', true],
  [AclPermissionType.ANY, 1, 'any', false],
  [AclPermissionType.DENY, 2, 'deny', false],
  [AclPermissionType.ALLOW, 3, 'allow', false],
];

test('testIsUnknown', () => {
  for (const [ty, , , unknown] of INFOS) {
    assert.equal(ty.isUnknown(), unknown, `${ty} was supposed to have unknown == ${unknown}`);
  }
});

test('testCode', () => {
  assert.equal(AclPermissionType.values().length, INFOS.length);
  for (const [ty, code] of INFOS) {
    assert.equal(ty.code(), code, `${ty} was supposed to have code == ${code}`);
    assert.equal(AclPermissionType.fromCode(code), ty, `AclPermissionType.fromCode(${code}) was supposed to be ${ty}`);
  }
  assert.equal(AclPermissionType.fromCode(120), AclPermissionType.UNKNOWN);
});

test('testName', () => {
  for (const [ty, , name] of INFOS) {
    assert.equal(AclPermissionType.fromString(name), ty, `AclPermissionType.fromString(${name}) was supposed to be ${ty}`);
  }
  assert.equal(AclPermissionType.fromString('something'), AclPermissionType.UNKNOWN);
});

test('testExhaustive', () => {
  assert.equal(INFOS.length, AclPermissionType.values().length);
  for (let i = 0; i < INFOS.length; i++) assert.equal(AclPermissionType.values()[i], INFOS[i][0]);
});

test('the enum members, the forms and the JSON form', () => {
  assert.equal(AclPermissionType.DENY.name(), 'DENY');
  assert.equal(AclPermissionType.DENY.ordinal(), 2);
  assert.equal(String(AclPermissionType.DENY), 'DENY');
  assert.equal(JSON.stringify(AclPermissionType.DENY), '"DENY"');
  assert.equal(AclPermissionType.valueOf({ name: 'ALLOW' }), AclPermissionType.ALLOW);
  assert.throws(() => AclPermissionType.valueOf('allow'), {
    name: 'IllegalArgumentError',
    message: 'No enum constant org.apache.kafka.common.acl.AclPermissionType.allow',
  });
  assert.equal(AclPermissionType.fromString({ str: 'Deny' }), AclPermissionType.DENY);
  // @ts-expect-error a null string: Java dereferences it, a TypeError
  assert.throws(() => AclPermissionType.fromString(null), { name: 'TypeError', message: 'str must not be null' });
  assert.throws(() => AclPermissionType.fromCode(-129), { name: 'RangeError', message: 'code must be from -128 to 127' });
  // @ts-expect-error values() only reads
  assert.throws(() => AclPermissionType.values(1), { name: 'TypeError', message: 'values() only reads: it takes no argument' });
  assert.ok(Object.isFrozen(AclPermissionType.ALLOW));
});
