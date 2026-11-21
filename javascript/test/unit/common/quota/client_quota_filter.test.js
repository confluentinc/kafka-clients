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

// ClientQuotaFilter: Java ships no test of it; this checks what Java's source
// does (its three factories, equals, toString).

const assert = require('node:assert/strict');

const { ClientQuotaFilter, ClientQuotaFilterComponent } = require('@confluentinc/kafka-javascript/common/quota');

test("contains, containsOnly and all: Java's strictness, equals and toString", () => {
  const c = ClientQuotaFilterComponent.ofEntity({ entityType: 'user', entityName: 'a' });
  const contains = ClientQuotaFilter.contains([c]);
  assert.equal(contains.strict(), false);
  assert.equal(ClientQuotaFilter.containsOnly({ components: [c] }).strict(), true);
  assert.ok(contains.equals(ClientQuotaFilter.contains([ClientQuotaFilterComponent.ofEntity({ entityType: 'user', entityName: 'a' })])));
  assert.ok(!contains.equals(ClientQuotaFilter.containsOnly([c])));
  assert.equal(contains.hashCode(), ClientQuotaFilter.contains([c]).hashCode());
  assert.equal(ClientQuotaFilter.all().toString(), 'ClientQuotaFilter(components=[], strict=false)');
  assert.equal(
    contains.toString(),
    'ClientQuotaFilter(components=[ClientQuotaFilterComponent(entityType=user, match=Optional[a])], strict=false)',
  );
  assert.notEqual(contains.components(), contains.components());
  // @ts-expect-error a null the types forbid: Java stores it
  assert.equal(ClientQuotaFilter.contains(null).components(), null);
  // @ts-expect-error a string where Java takes a Collection
  assert.throws(() => ClientQuotaFilter.contains('c'), {
    name: 'TypeError',
    message: 'components must be an iterable of ClientQuotaFilterComponent, not a string',
  });
});
