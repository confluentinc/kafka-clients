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

// Java's AclOperationTest.

const assert = require('node:assert/strict');

const { AclOperation } = require('@confluentinc/kafka-javascript/common/acl');

/** @type {Array<[import('@confluentinc/kafka-javascript/common/acl').AclOperation, number, string, boolean]>} */
const INFOS = [
  [AclOperation.UNKNOWN, 0, 'unknown', true],
  [AclOperation.ANY, 1, 'any', false],
  [AclOperation.ALL, 2, 'all', false],
  [AclOperation.READ, 3, 'read', false],
  [AclOperation.WRITE, 4, 'write', false],
  [AclOperation.CREATE, 5, 'create', false],
  [AclOperation.DELETE, 6, 'delete', false],
  [AclOperation.ALTER, 7, 'alter', false],
  [AclOperation.DESCRIBE, 8, 'describe', false],
  [AclOperation.CLUSTER_ACTION, 9, 'cluster_action', false],
  [AclOperation.DESCRIBE_CONFIGS, 10, 'describe_configs', false],
  [AclOperation.ALTER_CONFIGS, 11, 'alter_configs', false],
  [AclOperation.IDEMPOTENT_WRITE, 12, 'idempotent_write', false],
  [AclOperation.CREATE_TOKENS, 13, 'create_tokens', false],
  [AclOperation.DESCRIBE_TOKENS, 14, 'describe_tokens', false],
  [AclOperation.TWO_PHASE_COMMIT, 15, 'two_phase_commit', false],
];

test('testIsUnknown', () => {
  for (const [operation, , , unknown] of INFOS) {
    assert.equal(operation.isUnknown(), unknown, `${operation} was supposed to have unknown == ${unknown}`);
  }
});

test('testCode', () => {
  assert.equal(AclOperation.values().length, INFOS.length);
  for (const [operation, code] of INFOS) {
    assert.equal(operation.code(), code, `${operation} was supposed to have code == ${code}`);
    assert.equal(AclOperation.fromCode(code), operation, `AclOperation.fromCode(${code}) was supposed to be ${operation}`);
  }
  assert.equal(AclOperation.fromCode(120), AclOperation.UNKNOWN);
});

test('testName', () => {
  for (const [operation, , name] of INFOS) {
    assert.equal(AclOperation.fromString(name), operation, `AclOperation.fromString(${name}) was supposed to be ${operation}`);
  }
  assert.equal(AclOperation.fromString('something'), AclOperation.UNKNOWN);
});

test('testExhaustive', () => {
  assert.equal(INFOS.length, AclOperation.values().length);
  for (let i = 0; i < INFOS.length; i++) assert.equal(AclOperation.values()[i], INFOS[i][0]);
});

test('the enum members, the forms and the JSON form', () => {
  assert.equal(AclOperation.READ.name(), 'READ');
  assert.equal(AclOperation.READ.ordinal(), 3);
  assert.equal(AclOperation.READ.toString(), 'READ');
  assert.equal(JSON.stringify(AclOperation.READ), '"READ"');
  assert.equal(AclOperation.valueOf('WRITE'), AclOperation.WRITE);
  assert.equal(AclOperation.valueOf({ name: 'WRITE' }), AclOperation.WRITE);
  assert.throws(() => AclOperation.valueOf('write'), {
    name: 'IllegalArgumentError',
    message: 'No enum constant org.apache.kafka.common.acl.AclOperation.write',
  });
  assert.equal(AclOperation.fromCode({ code: 4 }), AclOperation.WRITE);
  assert.throws(() => AclOperation.fromCode(128), { name: 'RangeError', message: 'code must be from -128 to 127' });
  // @ts-expect-error a null string: Java dereferences it, a TypeError
  assert.throws(() => AclOperation.fromString(null), { name: 'TypeError', message: 'str must not be null' });
  assert.ok(Object.isFrozen(AclOperation.ANY));
});
