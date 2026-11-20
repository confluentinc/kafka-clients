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

// CreateTopicsResult and its TopicMetadataAndConfig: Java ships no test of
// them; the nested class's two constructors and the failure its accessors
// throw, Java's protected constructor and values()' dependants.

const assert = require('node:assert/strict');

const { Config, CreateTopicsResult } = require('@confluentinc/kafka-javascript/admin');
const { KafkaFuture, Uuid } = require('@confluentinc/kafka-javascript/common');
const { createFuture, internalFuture } = require('../../_internal');

/**
 * @template T
 * @typedef {import('@confluentinc/kafka-javascript/common').KafkaFuture<T>} Future
 */
const { TopicExistsError } = require('@confluentinc/kafka-javascript/common/errors');

test('TopicMetadataAndConfig: the values, or the failure each accessor throws', () => {
  const id = Uuid.randomUuid();
  const config = new Config([]);
  const metadata = new CreateTopicsResult.TopicMetadataAndConfig({ topicId: id, numPartitions: 3, replicationFactor: 2, config });
  assert.equal(metadata.topicId(), id);
  assert.equal(metadata.numPartitions(), 3);
  assert.equal(metadata.replicationFactor(), 2);
  assert.equal(metadata.config(), config);
  const error = new TopicExistsError('Topic t already exists.');
  const failed = new CreateTopicsResult.TopicMetadataAndConfig({ exception: error });
  for (const read of [() => failed.topicId(), () => failed.numPartitions(), () => failed.replicationFactor(), () => failed.config()]) {
    assert.throws(read, (thrown) => thrown === error);
  }
});

// A subclass reaches Java's protected constructor, as Java's tests in the
// package do.
class Subclass extends CreateTopicsResult {
  /** @param {Map<string, Future<CreateTopicsResult.TopicMetadataAndConfig>>} futures */
  constructor(futures) {
    super(futures);
  }
}

test("Java's protected constructor: the refinements read each topic's TopicMetadataAndConfig", async () => {
  const id = Uuid.randomUuid();
  const config = new Config([]);
  const metadata = new CreateTopicsResult.TopicMetadataAndConfig({ topicId: id, numPartitions: 3, replicationFactor: 2, config });
  const result = new Subclass(new Map([['t', KafkaFuture.completedFuture({ value: metadata })]]));
  assert.equal(await result.topicId('t').get(), id);
  assert.equal(await result.numPartitions('t').get(), 3);
  assert.equal(await result.replicationFactor({ topic: 't' }).get(), 2);
  assert.equal(await result.config('t').get(), config);
  assert.equal(await /** @type {Future<void>} */ (result.values().get('t')).get(), null);
  assert.ok(result.all().isDone());
  // Java's futures.get(topic) is null for a topic not in the map, which its thenApply dereferences.
  assert.throws(() => result.config('other'), { name: 'TypeError', message: 'futures has no future for topic other' });
  // A failed topic: each refinement fails with its error.
  const error = new TopicExistsError('Topic t already exists.');
  const failed = new Subclass(
    new Map([['t', KafkaFuture.completedFuture({ value: new CreateTopicsResult.TopicMetadataAndConfig({ exception: error }) })]]),
  );
  await assert.rejects(failed.config('t').get(), (thrown) => /** @type {Error} */ (thrown).cause === error);
});

test("values() hands out new dependants on every call: cancelling one leaves all() and the next values() alone, as Java's", async () => {
  const source = /** @type {Future<CreateTopicsResult.TopicMetadataAndConfig>} */ (createFuture(false));
  const result = new Subclass(new Map([['t', source]]));
  const first = /** @type {Future<void>} */ (result.values().get('t'));
  assert.notEqual(first, result.values().get('t'));
  first.cancel(true);
  assert.ok(first.isCancelled());
  assert.ok(!(/** @type {Future<void>} */ (result.values().get('t')).isCancelled()));
  const all = result.all();
  assert.ok(!all.isDone());
  internalFuture(source).internalComplete(
    new CreateTopicsResult.TopicMetadataAndConfig({ topicId: Uuid.ZERO_UUID, numPartitions: 1, replicationFactor: 1, config: null }),
    null,
  );
  assert.equal(await all.get(), null);
});

test('TopicMetadataAndConfig: the JSON form, or its failure as its text', () => {
  const metadata = new CreateTopicsResult.TopicMetadataAndConfig({
    topicId: Uuid.ZERO_UUID,
    numPartitions: 1,
    replicationFactor: 1,
    config: null,
  });
  assert.deepEqual(JSON.parse(JSON.stringify(metadata)), {
    topicId: Uuid.ZERO_UUID.toString(),
    numPartitions: 1,
    replicationFactor: 1,
    config: null,
    exception: null,
  });
  const failed = new CreateTopicsResult.TopicMetadataAndConfig({ exception: new TopicExistsError('Topic t already exists.') });
  assert.match(JSON.parse(JSON.stringify(failed)).exception, /Topic t already exists\./);
});
