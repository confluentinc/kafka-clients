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

// Java's ListTransactionsResultTest, over Java's constructor shape (the
// library's internalCreate over a future of a map of futures by broker), and
// the forms of the Result's readers.

const assert = require('node:assert/strict');

const { ExecutionError } = require('@confluentinc/kafka-javascript');
const { ListTransactionsResult, TransactionListing, TransactionState } = require('@confluentinc/kafka-javascript/admin');
const { KafkaError } = require('@confluentinc/kafka-javascript/common');
const { createFuture, internalResult } = require('../../_internal');

/** @typedef {import('@confluentinc/kafka-javascript/admin').TransactionListing} Listing */

function setUp() {
  /** @type {import('../../_internal').InternalFuture<Map<number, import('@confluentinc/kafka-javascript/common').KafkaFuture<Listing[]>>>} */
  const future = createFuture(false);
  const result = internalResult(ListTransactionsResult, future);
  return { future, result };
}

/**
 * Java's TestUtils.assertFutureThrows: the future fails with exactly `cls`.
 *
 * @param {Function} cls
 * @param {import('@confluentinc/kafka-javascript/common').KafkaFuture<unknown>} future
 */
async function assertFutureThrows(cls, future) {
  await assert.rejects(future.get(), (/** @type {unknown} */ e) => {
    assert.ok(e instanceof ExecutionError);
    assert.equal(/** @type {object} */ (e.cause).constructor, cls);
    return true;
  });
}

const broker1Listings = () => [
  new TransactionListing({ transactionalId: 'foo', producerId: 12345n, transactionState: TransactionState.ONGOING }),
  new TransactionListing({ transactionalId: 'bar', producerId: 98765n, transactionState: TransactionState.PREPARE_ABORT }),
];

test('testAllFuturesFailIfLookupFails', async () => {
  const { future, result } = setUp();
  future.internalComplete(/** @type {never} */ (null), new KafkaError());
  await assertFutureThrows(KafkaError, result.all());
  await assertFutureThrows(KafkaError, result.allByBrokerId());
  await assertFutureThrows(KafkaError, result.byBrokerId());
});

test('testAllFuturesSucceed', async () => {
  const { future, result } = setUp();
  /** @type {import('../../_internal').InternalFuture<Listing[]>} */
  const future1 = createFuture(false);
  /** @type {import('../../_internal').InternalFuture<Listing[]>} */
  const future2 = createFuture(false);
  future.internalComplete(
    new Map([
      [1, future1],
      [2, future2],
    ]),
    null,
  );
  const b1 = broker1Listings();
  future1.internalComplete(b1, null);
  const b2 = [new TransactionListing({ transactionalId: 'baz', producerId: 13579n, transactionState: TransactionState.COMPLETE_COMMIT })];
  future2.internalComplete(b2, null);

  const resultBrokerFutures = await result.byBrokerId().get();
  assert.deepEqual([...resultBrokerFutures.keys()].sort(), [1, 2]);
  assert.deepEqual(await resultBrokerFutures.get(1)?.get(), b1);
  assert.deepEqual(await resultBrokerFutures.get(2)?.get(), b2);
  assert.deepEqual((await result.allByBrokerId().get()).get(1), b1);
  assert.deepEqual((await result.allByBrokerId().get()).get(2), b2);

  const all = await result.all().get();
  assert.equal(all.length, 3);
  for (const expected of [...b1, ...b2])
    assert.ok(
      all.some((l) => l.equals(expected)),
      String(expected),
    );
});

test('testPartialFailure', async () => {
  const { future, result } = setUp();
  /** @type {import('../../_internal').InternalFuture<Listing[]>} */
  const future1 = createFuture(false);
  /** @type {import('../../_internal').InternalFuture<Listing[]>} */
  const future2 = createFuture(false);
  future.internalComplete(
    new Map([
      [1, future1],
      [2, future2],
    ]),
    null,
  );
  const b1 = broker1Listings();
  future1.internalComplete(b1, null);
  future2.internalComplete(/** @type {never} */ (null), new KafkaError());

  const resultBrokerFutures = await result.byBrokerId().get();
  // Ensure that the future for broker 1 completes successfully
  assert.deepEqual([...resultBrokerFutures.keys()].sort(), [1, 2]);
  assert.deepEqual(await resultBrokerFutures.get(1)?.get(), b1);
  // Everything else should fail
  await assertFutureThrows(KafkaError, result.all());
  await assertFutureThrows(KafkaError, result.allByBrokerId());
  await assertFutureThrows(
    KafkaError,
    /** @type {import('@confluentinc/kafka-javascript/common').KafkaFuture<unknown>} */ (resultBrokerFutures.get(2)),
  );
});

test("byBrokerId() and allByBrokerId() are new futures each call, and byBrokerId()'s map a copy", async () => {
  const { future, result } = setUp();
  assert.notEqual(result.byBrokerId(), result.byBrokerId());
  assert.notEqual(result.allByBrokerId(), result.allByBrokerId());
  future.internalComplete(new Map(), null);
  const a = await result.byBrokerId().get();
  a.set(9, /** @type {never} */ (null));
  assert.equal((await result.byBrokerId().get()).size, 0);
  // Java completes nothing for an empty map: allByBrokerId() stays pending.
  assert.equal(result.allByBrokerId().isDone(), false);
});

test('the readers refuse an argument', () => {
  const { result } = setUp();
  for (const name of /** @type {const} */ (['all', 'byBrokerId', 'allByBrokerId'])) {
    // @ts-expect-error a reader takes no argument
    assert.throws(() => result[name](1), { name: 'TypeError', message: `${name}() only reads: it takes no argument` });
  }
});
