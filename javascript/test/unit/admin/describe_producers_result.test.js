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

// DescribeProducersResult and its nested PartitionProducerState, which Java
// ships without tests: partitionResult's lookup and Java's message for a
// partition not requested, all()'s allOf composition, and the nested class's
// public constructor (a null stored, as Java's stores it) and toString.

const assert = require('node:assert/strict');

const { DescribeProducersResult, ProducerState } = require('@confluentinc/kafka-javascript/admin');
const { KafkaError, TopicPartition, TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');
const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { createFuture, internalResult } = require('../../_internal');

/**
 * The future fails with an ExecutionError whose cause is a `cls` of `message`.
 *
 * @param {import('@confluentinc/kafka-javascript/common').KafkaFuture<unknown> | undefined} future
 * @param {Function} cls
 * @param {string} message
 */
async function rejectsWith(future, cls, message) {
  assert.ok(future);
  await assert.rejects(future.get(), (/** @type {unknown} */ e) => {
    assert.ok(e instanceof ExecutionError);
    assert.ok(e.cause instanceof cls);
    assert.equal(/** @type {Error} */ (e.cause).message, message);
    return true;
  });
}

const tp0 = new TopicPartition({ topic: 't', partition: 0 });
const tp1 = new TopicPartition({ topic: 't', partition: 1 });
const producer = new ProducerState({
  producerId: 1n,
  producerEpoch: 0,
  lastSequence: 5,
  lastTimestamp: 10n,
  coordinatorEpoch: null,
  currentTransactionStartOffset: null,
});

function setUp() {
  /** @type {import('../../_internal').InternalFuture<DescribeProducersResult.PartitionProducerState>} */
  const f0 = createFuture(false);
  /** @type {import('../../_internal').InternalFuture<DescribeProducersResult.PartitionProducerState>} */
  const f1 = createFuture(false);
  const result = internalResult(
    DescribeProducersResult,
    new TopicPartitionMap([
      [tp0, f0],
      [tp1, f1],
    ]),
  );
  return { f0, f1, result };
}

test("partitionResult finds a partition by value, and throws Java's message for one not requested", () => {
  const { f0, result } = setUp();
  assert.equal(result.partitionResult(new TopicPartition({ topic: 't', partition: 0 })), f0);
  assert.equal(result.partitionResult({ partition: tp0 }), f0);
  assert.throws(() => result.partitionResult(new TopicPartition({ topic: 't', partition: 9 })), {
    name: 'IllegalArgumentError',
    message: 'Topic partition t-9 was not included in the request',
  });
});

test("all() is Java's allOf over the partitions' futures", async () => {
  const { f0, f1, result } = setUp();
  const all = result.all();
  const state = new DescribeProducersResult.PartitionProducerState([producer]);
  f0.internalComplete(state, null);
  assert.equal(all.isDone(), false);
  f1.internalComplete(new DescribeProducersResult.PartitionProducerState({ activeProducers: [] }), null);
  const map = await all.get();
  assert.equal(map.get(tp0), state);
  const failing = setUp();
  failing.f0.internalComplete(/** @type {never} */ (null), new KafkaError('boom'));
  failing.f1.internalComplete(state, null);
  await rejectsWith(failing.result.all(), KafkaError, 'boom');
});

test("PartitionProducerState's constructor stores Java's list, a null included, and its toString is Java's", () => {
  const state = new DescribeProducersResult.PartitionProducerState([producer]);
  assert.deepEqual(state.activeProducers(), [producer]);
  assert.notEqual(state.activeProducers(), state.activeProducers());
  assert.equal(
    String(state),
    'PartitionProducerState(activeProducers=[ProducerState(producerId=1, producerEpoch=0, lastSequence=5, lastTimestamp=10, coordinatorEpoch=OptionalInt.empty, currentTransactionStartOffset=OptionalLong.empty)])',
  );
  const empty = new DescribeProducersResult.PartitionProducerState(null);
  assert.equal(empty.activeProducers(), null);
  assert.equal(String(empty), 'PartitionProducerState(activeProducers=null)');
});
