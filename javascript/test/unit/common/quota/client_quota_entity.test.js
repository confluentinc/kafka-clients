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

// ClientQuotaEntity: Java ships no test of it, so these are the Rust core's
// test of its translation (rust/src/common/quota/client_quota_entity.rs) and
// what Java's source does (equals, hashCode, toString, the entity types).

const assert = require('node:assert/strict');

const { ClientQuotaEntity } = require('@confluentinc/kafka-javascript/common/quota');

test("Java's constants and isValidEntityType", () => {
  assert.deepEqual([ClientQuotaEntity.USER, ClientQuotaEntity.CLIENT_ID, ClientQuotaEntity.IP], ['user', 'client-id', 'ip']);
  for (const t of ['user', 'client-id', 'ip']) assert.equal(ClientQuotaEntity.isValidEntityType(t), true);
  assert.equal(ClientQuotaEntity.isValidEntityType('USER'), false);
  assert.equal(ClientQuotaEntity.isValidEntityType({ entityType: null }), false);
});

test('equal entries are equal, whatever their order; a null name is the default entity', () => {
  const a = new ClientQuotaEntity(
    new Map([
      ['user', 'alice'],
      ['client-id', null],
    ]),
  );
  const b = new ClientQuotaEntity({ entries: { 'client-id': null, user: 'alice' } });
  assert.ok(a.equals(b));
  assert.equal(a.hashCode(), b.hashCode());
  assert.ok(!a.equals(new ClientQuotaEntity(new Map([['user', 'alice']]))));
  assert.equal(a.toString(), 'ClientQuotaEntity(entries={user=alice, client-id=null})');
  // The entity holds a copy; entries() hands out a new one.
  const given = new Map([['user', 'bob']]);
  const e = new ClientQuotaEntity(given);
  given.set('user', 'carol');
  assert.equal(e.entries().get('user'), 'bob');
  e.entries().set('user', 'dave');
  assert.equal(e.entries().get('user'), 'bob');
  assert.deepEqual(JSON.parse(JSON.stringify(a)), { entries: { user: 'alice', 'client-id': null } });
});

test('default_entity_none_distinct_from_empty_name', () => {
  const defaultUser = new ClientQuotaEntity(new Map([[ClientQuotaEntity.USER, null]]));
  const emptyNamedUser = new ClientQuotaEntity(new Map([[ClientQuotaEntity.USER, '']]));
  assert.equal(defaultUser.entries().get(ClientQuotaEntity.USER), null);
  assert.equal(emptyNamedUser.entries().get(ClientQuotaEntity.USER), '');
  assert.ok(!defaultUser.equals(emptyNamedUser));
});

test('a null map is stored, as Java stores it', () => {
  // @ts-expect-error a null the types forbid: Java stores it
  const e = new ClientQuotaEntity(null);
  assert.equal(e.entries(), null);
  // @ts-expect-error a null the types forbid: Java stores it
  assert.ok(e.equals(new ClientQuotaEntity(null)));
  assert.equal(e.toString(), 'ClientQuotaEntity(entries=null)');
  // @ts-expect-error a number key
  assert.throws(() => new ClientQuotaEntity(new Map([[1, 'a']])), {
    name: 'TypeError',
    message: 'entries must have string keys, not a number',
  });
});
