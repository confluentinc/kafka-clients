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

// FenceProducersResult, which Java ships without tests: fencedProducers(),
// producerId() and epochId() as Java's thenApply over each id's future, new
// futures each call, Java's message for an id not requested, and all().

const assert = require('node:assert/strict');

const { FenceProducersResult } = require('@confluentinc/kafka-javascript/admin');
const { KafkaError } = require('@confluentinc/kafka-javascript/common');
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

test("each reader composes over the id's producer id and epoch", async () => {
  /** @type {import('../../_internal').InternalFuture<{ producerId: bigint, epoch: number }>} */
  const future = createFuture(false);
  const result = internalResult(FenceProducersResult, new Map([['foo', future]]));
  const fenced = result.fencedProducers();
  assert.deepEqual([...fenced.keys()], ['foo']);
  assert.notEqual(result.fencedProducers().get('foo'), fenced.get('foo'));
  const pid = result.producerId('foo');
  const epoch = result.epochId({ transactionalId: 'foo' });
  future.internalComplete({ producerId: 12345n, epoch: 7 }, null);
  assert.equal(await pid.get(), 12345n);
  assert.equal(await epoch.get(), 7);
  assert.equal(await fenced.get('foo')?.get(), null);
  assert.equal(await result.all().get(), null);
  for (const name of /** @type {const} */ (['producerId', 'epochId'])) {
    assert.throws(() => result[name]('bar'), {
      name: 'IllegalArgumentError',
      message: 'TransactionalId `bar` was not included in the request',
    });
  }
});

test('a failed fencing fails every reader', async () => {
  /** @type {import('../../_internal').InternalFuture<{ producerId: bigint, epoch: number }>} */
  const future = createFuture(false);
  const result = internalResult(FenceProducersResult, new Map([['foo', future]]));
  future.internalComplete(/** @type {never} */ (null), new KafkaError('fenced'));
  for (const f of [result.producerId('foo'), result.epochId('foo'), result.all(), result.fencedProducers().get('foo')]) {
    await rejectsWith(f, KafkaError, 'fenced');
  }
});
