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

// Java's DescribeTopicsResultTest, and the all-futures' maps. Java's
// package-private factories are the @internal statics internalOfTopicNames
// and internalOfTopicIds.

const assert = require('node:assert/strict');

const { DescribeTopicsResult, TopicDescription } = require('@confluentinc/kafka-javascript/admin');
const { KafkaFuture, Uuid, UuidMap } = require('@confluentinc/kafka-javascript/common');
const { topicResultFactories } = require('../../_internal');

const factories = topicResultFactories(DescribeTopicsResult);

const description = () => new TopicDescription({ name: 'foo', internal: false, partitions: [] });

test('testNullAllTopicNames', () => {
  const result = factories.internalOfTopicIds(new UuidMap([[Uuid.randomUuid(), KafkaFuture.completedFuture({ value: description() })]]));
  assert.equal(result.allTopicNames(), null);
});

test('testNullAllTopicIds', () => {
  const result = factories.internalOfTopicNames(new Map([['foo', KafkaFuture.completedFuture({ value: description() })]]));
  assert.equal(result.allTopicIds(), null);
});

// ---- the binding's own ----------------------------------------------------------

test("allTopicNames and allTopicIds map each key to its future's value, and fail with the first failure", async () => {
  const foo = description();
  const byName = factories.internalOfTopicNames(new Map([['foo', KafkaFuture.completedFuture({ value: foo })]]));
  const names = await /** @type {KafkaFuture<Map<string, TopicDescription>>} */ (byName.allTopicNames()).get();
  assert.deepEqual([...names], [['foo', foo]]);
  const id = Uuid.randomUuid();
  const byId = factories.internalOfTopicIds(new UuidMap([[id, KafkaFuture.completedFuture({ value: foo })]]));
  const ids = await /** @type {KafkaFuture<UuidMap<TopicDescription>>} */ (byId.allTopicIds()).get();
  assert.equal(ids.get(new Uuid({ mostSigBits: id.getMostSignificantBits(), leastSigBits: id.getLeastSignificantBits() })), foo);
});

test('the readers only read: an argument is a TypeError', () => {
  const result = factories.internalOfTopicNames(new Map([['foo', KafkaFuture.completedFuture({ value: description() })]]));
  // Called with an argument tsc refuses, as a plain JavaScript caller calls them.
  const loose = /** @type {Record<string, (x: unknown) => unknown>} */ (/** @type {unknown} */ (result));
  for (const name of ['topicIdValues', 'topicNameValues', 'allTopicNames', 'allTopicIds']) {
    assert.throws(() => loose[name](1), { name: 'TypeError', message: `${name}() only reads: it takes no argument` });
  }
});

test("TopicDescription's JSON form: the authorized operations as an array, null when not known", () => {
  const { AclOperation } = require('@confluentinc/kafka-javascript/common/acl');
  const known = new TopicDescription({
    name: 'foo',
    internal: false,
    partitions: [],
    authorizedOperations: [AclOperation.READ, AclOperation.WRITE],
  });
  assert.deepEqual(JSON.parse(JSON.stringify(known)), {
    name: 'foo',
    isInternal: false,
    topicId: Uuid.ZERO_UUID.toString(),
    partitions: [],
    authorizedOperations: ['READ', 'WRITE'],
  });
  const unknown = new TopicDescription({ name: 'foo', internal: false, partitions: [], authorizedOperations: null });
  assert.equal(JSON.parse(JSON.stringify(unknown)).authorizedOperations, null);
});
