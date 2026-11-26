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

// Java's DeleteConsumerGroupOffsetsResultTest, over Java's constructor shape
// (the library's internalCreate, each partition's error null for Java's
// Errors.NONE).

const assert = require('node:assert/strict');

const { ExecutionError, IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { TopicPartition, TopicPartitionMap, TopicPartitionSet } = require('@confluentinc/kafka-javascript/common');
const { GroupAuthorizationError, UnknownTopicOrPartitionError } = require('@confluentinc/kafka-javascript/common/errors');
const { createFuture, deleteConsumerGroupOffsetsResult } = require('../../_internal');

const topic = 'topic';
const tpZero = new TopicPartition({ topic, partition: 0 });
const tpOne = new TopicPartition({ topic, partition: 1 });

function setUp() {
  /** @type {import('../../_internal').InternalFuture<TopicPartitionMap<Error | null>>} */
  const partitionFutures = createFuture(false);
  const partitions = new TopicPartitionSet([tpZero, tpOne]);
  const errorsMap = new TopicPartitionMap();
  errorsMap.set(tpZero, null);
  errorsMap.set(tpOne, new UnknownTopicOrPartitionError('unknown'));
  return { partitionFutures, partitions, errorsMap: /** @type {TopicPartitionMap<Error | null>} */ (errorsMap) };
}

/**
 * @param {Function} cls
 * @param {import('@confluentinc/kafka-javascript/common').KafkaFuture<unknown>} future
 */
async function assertFutureThrows(cls, future) {
  await assert.rejects(future.get(), (e) => e instanceof ExecutionError && e.cause instanceof cls);
}

test('testTopLevelErrorConstructor', async () => {
  const s = setUp();
  s.partitionFutures.internalComplete(new TopicPartitionMap(), new GroupAuthorizationError('auth'));
  await assertFutureThrows(GroupAuthorizationError, deleteConsumerGroupOffsetsResult(s.partitionFutures, s.partitions).all());
});

/**
 * @param {ReturnType<typeof setUp>} s
 */
async function createAndVerifyPartitionLevelError(s) {
  s.partitionFutures.internalComplete(s.errorsMap, null);
  assert.ok(!s.partitionFutures.isCompletedExceptionally());
  const result = deleteConsumerGroupOffsetsResult(s.partitionFutures, s.partitions);
  await assertFutureThrows(UnknownTopicOrPartitionError, result.all());
  assert.equal(await result.partitionResult(tpZero).get(), null);
  await assertFutureThrows(UnknownTopicOrPartitionError, result.partitionResult(tpOne));
  return result;
}

test('testPartitionLevelErrorConstructor', async () => {
  await createAndVerifyPartitionLevelError(setUp());
});

test('testPartitionMissingInResponseErrorConstructor', async () => {
  const s = setUp();
  s.errorsMap.delete(tpOne);
  s.partitionFutures.internalComplete(s.errorsMap, null);
  const result = deleteConsumerGroupOffsetsResult(s.partitionFutures, s.partitions);
  await assertFutureThrows(IllegalArgumentError, result.all());
  assert.equal(await result.partitionResult(tpZero).get(), null);
  await assert.rejects(
    result.partitionResult(tpOne).get(),
    (e) =>
      e instanceof ExecutionError &&
      e.cause instanceof IllegalArgumentError &&
      e.cause.message === 'Offset deletion result for partition "topic-1" was not included in the response',
  );
});

test('testPartitionMissingInRequestErrorConstructor', async () => {
  const result = await createAndVerifyPartitionLevelError(setUp());
  assert.throws(() => result.partitionResult(new TopicPartition({ topic: 'invalid-topic', partition: 0 })), {
    name: 'IllegalArgumentError',
    message: 'Partition invalid-topic-0 was not included in the original request',
  });
});

test('testNoErrorConstructor', async () => {
  const s = setUp();
  const result = deleteConsumerGroupOffsetsResult(s.partitionFutures, s.partitions);
  const none = new TopicPartitionMap();
  none.set(tpZero, null);
  none.set(tpOne, null);
  s.partitionFutures.internalComplete(none, null);
  assert.equal(await result.all().get(), null);
  assert.equal(await result.partitionResult(tpZero).get(), null);
  assert.equal(await result.partitionResult(tpOne).get(), null);
});
