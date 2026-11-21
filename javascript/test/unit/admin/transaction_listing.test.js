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

// TransactionListing, which Java ships without tests: its getters, Java's
// equals and toString, and toJSON.

const assert = require('node:assert/strict');

const { TransactionListing, TransactionState } = require('@confluentinc/kafka-javascript/admin');

test("the getters, Java's equals and toString", () => {
  const l = new TransactionListing({ transactionalId: 'foo', producerId: 12345n, transactionState: TransactionState.ONGOING });
  assert.equal(l.transactionalId(), 'foo');
  assert.equal(l.producerId(), 12345n);
  assert.equal(l.state(), TransactionState.ONGOING);
  assert.equal(String(l), "TransactionListing(transactionalId='foo', producerId=12345, transactionState=Ongoing)");
  const same = new TransactionListing({ transactionalId: 'foo', producerId: 12345, transactionState: TransactionState.ONGOING });
  assert.ok(l.equals(same));
  assert.equal(l.hashCode(), same.hashCode());
  assert.ok(!l.equals(new TransactionListing({ transactionalId: 'foo', producerId: 12345n, transactionState: TransactionState.EMPTY })));
  assert.deepEqual(JSON.parse(JSON.stringify(l)), { transactionalId: 'foo', producerId: '12345', transactionState: 'ONGOING' });
});

test('a null id and state are stored, as Java stores them', () => {
  const l = new TransactionListing({ transactionalId: null, producerId: 1n, transactionState: null });
  assert.equal(String(l), "TransactionListing(transactionalId='null', producerId=1, transactionState=null)");
  // @ts-expect-error a state of the wrong type
  assert.throws(() => new TransactionListing({ transactionalId: 'a', producerId: 1n, transactionState: 'Ongoing' }), {
    name: 'TypeError',
    message: 'transactionState must be a TransactionState or null, not a string',
  });
});
