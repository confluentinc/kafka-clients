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

// ClientQuotaFilterComponent: Java's
// KafkaAdminClientConfigTest#testEqualsOfClientQuotaFilterComponent, which
// needs no MockClient (the core ports it too, in
// rust/src/common/quota/client_quota_filter_component.rs), and what Java's
// source does: its three factories, the Optional match (present, empty or
// null), equals and toString.

const assert = require('node:assert/strict');

const { ClientQuotaEntity, ClientQuotaFilterComponent } = require('@confluentinc/kafka-javascript/common/quota');

test('testEqualsOfClientQuotaFilterComponent', () => {
  const USER = ClientQuotaEntity.USER;
  assert.ok(ClientQuotaFilterComponent.ofDefaultEntity(USER).equals(ClientQuotaFilterComponent.ofDefaultEntity(USER)));
  assert.ok(ClientQuotaFilterComponent.ofEntityType(USER).equals(ClientQuotaFilterComponent.ofEntityType(USER)));
  // match = null is different from match = Empty
  assert.ok(!ClientQuotaFilterComponent.ofDefaultEntity(USER).equals(ClientQuotaFilterComponent.ofEntityType(USER)));
  assert.ok(
    ClientQuotaFilterComponent.ofEntity({ entityType: USER, entityName: 'user' }).equals(
      ClientQuotaFilterComponent.ofEntity({ entityType: USER, entityName: 'user' }),
    ),
  );
  assert.ok(
    !ClientQuotaFilterComponent.ofEntity({ entityType: USER, entityName: 'user' }).equals(ClientQuotaFilterComponent.ofDefaultEntity(USER)),
  );
  assert.ok(
    !ClientQuotaFilterComponent.ofEntity({ entityType: USER, entityName: 'user' }).equals(ClientQuotaFilterComponent.ofEntityType(USER)),
  );
});

test("the three matches: Java's toString keeps them apart; match() is null for the default and for any", () => {
  const exact = ClientQuotaFilterComponent.ofEntity({ entityType: 'user', entityName: 'alice' });
  const byDefault = ClientQuotaFilterComponent.ofDefaultEntity('user');
  const any = ClientQuotaFilterComponent.ofEntityType({ entityType: 'user' });
  assert.equal(exact.match(), 'alice');
  assert.equal(byDefault.match(), null);
  assert.equal(any.match(), null);
  assert.equal(exact.toString(), 'ClientQuotaFilterComponent(entityType=user, match=Optional[alice])');
  assert.equal(byDefault.toString(), 'ClientQuotaFilterComponent(entityType=user, match=Optional.empty)');
  assert.equal(any.toString(), 'ClientQuotaFilterComponent(entityType=user, match=null)');
  assert.ok(!byDefault.equals(any));
  assert.ok(byDefault.equals(ClientQuotaFilterComponent.ofDefaultEntity('user')));
  assert.equal(byDefault.hashCode(), any.hashCode());
  assert.deepEqual(JSON.parse(JSON.stringify(any)), { entityType: 'user', match: null, matchKind: 'any' });
});

test("Java's null checks: the name before the type (Optional.of(requireNonNull(entityName)))", () => {
  // @ts-expect-error both null
  assert.throws(() => ClientQuotaFilterComponent.ofEntity({ entityType: null, entityName: null }), {
    name: 'TypeError',
    message: 'entityName must not be null',
  });
  // @ts-expect-error a null type
  assert.throws(() => ClientQuotaFilterComponent.ofEntity({ entityType: null, entityName: 'a' }), {
    name: 'TypeError',
    message: 'entityType must not be null',
  });
  // @ts-expect-error a null type
  assert.throws(() => ClientQuotaFilterComponent.ofEntityType(null), { name: 'TypeError', message: 'entityType must not be null' });
});
