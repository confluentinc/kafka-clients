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

// DescribeTransactionsResult, which Java ships without tests: description's
// lookup and Java's message for an id not requested, and all()'s allOf
// composition, keyed by id.

const assert = require('node:assert/strict');

const { DescribeTransactionsResult, TransactionDescription, TransactionState } = require('@confluentinc/kafka-javascript/admin');
const { createFuture, internalResult } = require('../../_internal');

const description = new TransactionDescription({
  coordinatorId: 1,
  state: TransactionState.ONGOING,
  producerId: 2n,
  producerEpoch: 0,
  transactionTimeoutMs: 60000n,
  transactionStartTimeMs: null,
  topicPartitions: [],
});

test("description finds an id, and throws Java's message for one not requested; all() maps each id", async () => {
  /** @type {import('../../_internal').InternalFuture<TransactionDescription>} */
  const future = createFuture(false);
  const result = internalResult(DescribeTransactionsResult, new Map([['foo', future]]));
  assert.equal(result.description('foo'), future);
  assert.equal(result.description({ transactionalId: 'foo' }), future);
  assert.throws(() => result.description('bar'), {
    name: 'IllegalArgumentError',
    message: 'TransactionalId `bar` was not included in the request',
  });
  const all = result.all();
  future.internalComplete(description, null);
  assert.deepEqual([...(await all.get())], [['foo', description]]);
});
