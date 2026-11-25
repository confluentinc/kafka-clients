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

// AlterClientQuotasResult and DescribeClientQuotasResult, whose Java
// constructors are public: Java ships no test of them; this checks what
// Java's sources do (a null stored, all() over the futures).

const assert = require('node:assert/strict');

const { AlterClientQuotasResult } = require('@confluentinc/kafka-javascript/admin');
const { ClientQuotaEntity, ClientQuotaEntityMap } = require('@confluentinc/kafka-javascript/common/quota');
const { createFuture } = require('../../_internal');

test('all() is allOf over the futures; a null map is stored and all() dereferences it', async () => {
  /** @type {import('../../_internal').InternalFuture<void>} */
  const f = createFuture(false);
  const entity = new ClientQuotaEntity(new Map([['user', 'a']]));
  const futures = new ClientQuotaEntityMap([
    [entity, /** @type {import('@confluentinc/kafka-javascript/common').KafkaFuture<void>} */ (f)],
  ]);
  const result = new AlterClientQuotasResult(futures);
  assert.equal(result.values(), futures);
  const all = result.all();
  assert.equal(all.isDone(), false);
  f.internalComplete(undefined, null);
  await all.get();
  // @ts-expect-error a null the types forbid: Java stores it
  const empty = new AlterClientQuotasResult({ futures: null });
  assert.equal(empty.values(), null);
  assert.throws(() => empty.all(), { name: 'TypeError', message: 'futures must not be null' });
  assert.throws(
    // @ts-expect-error a plain Map where Java takes a Map<ClientQuotaEntity, ...>
    () => new AlterClientQuotasResult(new Map()),
    { name: 'TypeError', message: 'futures must be a ClientQuotaEntityMap, not a Map: build one with new ClientQuotaEntityMap()' },
  );
  assert.throws(
    // @ts-expect-error a plain Map where Java takes a Map<ClientQuotaEntity, ...>
    () => new AlterClientQuotasResult({ futures: new Map() }),
    { name: 'TypeError', message: 'futures must be a ClientQuotaEntityMap, not a Map: build one with new ClientQuotaEntityMap()' },
  );
});
