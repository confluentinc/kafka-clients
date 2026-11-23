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

// Java's DeleteTopicsResultTest. Java's package-private factories are the
// @internal statics internalOfTopicNames and internalOfTopicIds, and its
// protected constructor is reached through a subclass, as Java's test in the
// package reaches it.

const assert = require('node:assert/strict');

const { DeleteTopicsResult } = require('@confluentinc/kafka-javascript/admin');
const { KafkaFuture, Uuid, UuidMap } = require('@confluentinc/kafka-javascript/common');
const { topicResultFactories } = require('../../_internal');

const factories = topicResultFactories(DeleteTopicsResult);

/**
 * @template T
 * @typedef {import('@confluentinc/kafka-javascript/common').KafkaFuture<T>} Future
 */

/** A subclass that reaches Java's protected constructor. */
class Subclass extends DeleteTopicsResult {
  /**
   * @param {UuidMap<Future<void>> | null} topicIdFutures
   * @param {Map<string, Future<void>> | null} nameFutures
   */
  constructor(topicIdFutures, nameFutures) {
    super(topicIdFutures, nameFutures);
  }
}

test('testDeleteTopicsResultWithNames', () => {
  const future = KafkaFuture.completedFuture({ value: null });
  const topicNames = new Map([['foo', future]]);
  const topicNameFutures = factories.internalOfTopicNames(topicNames);
  assert.equal(topicNameFutures.topicNameValues(), topicNames);
  assert.equal(topicNameFutures.topicIdValues(), null);
  assert.ok(topicNameFutures.all().isDone());
});

test('testDeleteTopicsResultWithIds', () => {
  const future = KafkaFuture.completedFuture({ value: null });
  const topicIds = new UuidMap([[Uuid.randomUuid(), future]]);
  const topicIdFutures = factories.internalOfTopicIds(topicIds);
  assert.equal(topicIdFutures.topicIdValues(), topicIds);
  assert.equal(topicIdFutures.topicNameValues(), null);
  assert.ok(topicIdFutures.all().isDone());
});

test('testInvalidConfigurations', () => {
  assert.throws(() => new Subclass(null, null), {
    name: 'IllegalArgumentError',
    message: 'topicIdFutures and nameFutures cannot both be null.',
  });
  assert.throws(() => new Subclass(new UuidMap(), new Map()), {
    name: 'IllegalArgumentError',
    message: 'topicIdFutures and nameFutures cannot both be specified.',
  });
});

test('the readers only read: an argument is a TypeError', () => {
  const result = factories.internalOfTopicNames(new Map([['foo', KafkaFuture.completedFuture({ value: null })]]));
  // Called with an argument tsc refuses, as a plain JavaScript caller calls them.
  const loose = /** @type {Record<string, (x: unknown) => unknown>} */ (/** @type {unknown} */ (result));
  for (const name of ['topicIdValues', 'topicNameValues', 'all']) {
    assert.throws(() => loose[name](1), { name: 'TypeError', message: `${name}() only reads: it takes no argument` });
  }
});
