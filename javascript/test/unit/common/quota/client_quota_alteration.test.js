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

// ClientQuotaAlteration and its Op: Java ships no test of them; this checks
// what Java's source does (Op's equals and hashCode over a Double,
// toString).

const assert = require('node:assert/strict');

const { ClientQuotaAlteration, ClientQuotaEntity } = require('@confluentinc/kafka-javascript/common/quota');

test("Op: Java's Double.equals and Double.hashCode, toString with Java's double text", () => {
  const op = new ClientQuotaAlteration.Op({ key: 'producer_byte_rate', value: 1024 });
  assert.ok(op.equals(new ClientQuotaAlteration.Op({ key: 'producer_byte_rate', value: 1024 })));
  assert.ok(new ClientQuotaAlteration.Op({ key: 'k', value: NaN }).equals(new ClientQuotaAlteration.Op({ key: 'k', value: NaN })));
  assert.ok(!new ClientQuotaAlteration.Op({ key: 'k', value: 0 }).equals(new ClientQuotaAlteration.Op({ key: 'k', value: -0 })));
  // Objects.hash("k", 1.0): 31 * (31 + "k".hashCode()) + Double.hashCode(1.0) (1072693248).
  assert.equal(new ClientQuotaAlteration.Op({ key: 'k', value: 1 }).hashCode(), (Math.imul(31, 31 + 107) + 1072693248) | 0);
  assert.equal(op.toString(), 'ClientQuotaAlteration.Op(key=producer_byte_rate, value=1024.0)');
  assert.equal(new ClientQuotaAlteration.Op({ key: 'k', value: null }).toString(), 'ClientQuotaAlteration.Op(key=k, value=null)');
  assert.equal(op.key(), 'producer_byte_rate');
  assert.equal(op.value(), 1024);
});

test('an alteration holds a copy of its ops; Java has no equals for it, so it compares by identity', () => {
  const entity = new ClientQuotaEntity(new Map([['user', 'a']]));
  const ops = [new ClientQuotaAlteration.Op({ key: 'k', value: 1 })];
  const alteration = new ClientQuotaAlteration({ entity, ops });
  ops.push(new ClientQuotaAlteration.Op({ key: 'j', value: 2 }));
  assert.equal(alteration.ops().length, 1);
  assert.equal(alteration.entity(), entity);
  assert.equal(
    alteration.toString(),
    'ClientQuotaAlteration(entity=ClientQuotaEntity(entries={user=a}), ops=[ClientQuotaAlteration.Op(key=k, value=1.0)])',
  );
  // @ts-expect-error a null the types forbid: Java stores it
  assert.equal(new ClientQuotaAlteration({ entity: null, ops: null }).ops(), null);
});
