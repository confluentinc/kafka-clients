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

// ClientQuotaEntityMap (js-only): the map keyed by ClientQuotaEntity by
// value, as TopicPartitionMap is keyed by TopicPartition.

const assert = require('node:assert/strict');

const { ClientQuotaEntity, ClientQuotaEntityMap } = require('@confluentinc/kafka-javascript/common/quota');

test('an equal entity finds the entry, whatever the order of its entries; keys() are the original entities', () => {
  const key = new ClientQuotaEntity(
    new Map([
      ['user', 'a'],
      ['client-id', 'c'],
    ]),
  );
  const map = new ClientQuotaEntityMap([[key, 1]]);
  assert.equal(
    map.get(
      new ClientQuotaEntity(
        new Map([
          ['client-id', 'c'],
          ['user', 'a'],
        ]),
      ),
    ),
    1,
  );
  assert.equal(map.get(new ClientQuotaEntity(new Map([['user', 'a']]))), null);
  map.set(
    new ClientQuotaEntity(
      new Map([
        ['client-id', 'c'],
        ['user', 'a'],
      ]),
    ),
    2,
  );
  assert.equal(map.size(), 1);
  assert.equal([...map.keys()][0], key);
  assert.equal(map.toString(), '{ClientQuotaEntity(entries={user=a, client-id=c})=2}');
  assert.deepEqual(map.toJSON(), { '[["client-id","c"],["user","a"]]': 2 });
  assert.ok(map.delete(key));
  assert.equal(map.size(), 0);
  assert.throws(
    () => map.get(/** @type {ClientQuotaEntity} */ (/** @type {unknown} */ ('a'))),
    (e) => e instanceof Error,
  );
});
