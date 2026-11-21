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

// TransactionDescription, a data class Java ships without tests: its readonly
// fields, frozen, its read-only set of partitions (null stored, as Java's
// constructor stores it), Java's equals, hashCode and toString, and toJSON.

const assert = require('node:assert/strict');

const { TransactionDescription, TransactionState } = require('@confluentinc/kafka-javascript/admin');
const { TopicPartition, TopicPartitionSet } = require('@confluentinc/kafka-javascript/common');

const tp = new TopicPartition({ topic: 't', partition: 0 });
/** @param {Partial<ConstructorParameters<typeof TransactionDescription>[0]>} [over] */
const description = (over = {}) =>
  new TransactionDescription({
    coordinatorId: 1,
    state: TransactionState.ONGOING,
    producerId: 12345n,
    producerEpoch: 15,
    transactionTimeoutMs: 60000n,
    transactionStartTimeMs: 1000n,
    topicPartitions: [tp],
    ...over,
  });

test('the fields are readonly, the object frozen and its set read-only', () => {
  const d = description();
  assert.equal(d.coordinatorId, 1);
  assert.equal(d.state, TransactionState.ONGOING);
  assert.equal(d.producerId, 12345n);
  assert.equal(d.producerEpoch, 15);
  assert.equal(d.transactionTimeoutMs, 60000n);
  assert.equal(d.transactionStartTimeMs, 1000n);
  assert.ok(d.topicPartitions instanceof TopicPartitionSet);
  assert.ok(Object.isFrozen(d));
  // Java's unmodifiable set throws UnsupportedOperationException with no message.
  assert.throws(() => d.topicPartitions?.add(new TopicPartition({ topic: 'u', partition: 1 })), {
    name: 'UnsupportedOperationError',
    message: '',
  });
  assert.throws(() => d.topicPartitions?.clear(), { name: 'UnsupportedOperationError', message: '' });
  assert.equal(description({ topicPartitions: null }).topicPartitions, null);
  assert.equal(description({ transactionStartTimeMs: null }).transactionStartTimeMs, null);
});

test("toString, equals and hashCode are Java's", () => {
  assert.equal(
    String(description()),
    'TransactionDescription(coordinatorId=1, state=Ongoing, producerId=12345, producerEpoch=15, transactionTimeoutMs=60000, transactionStartTimeMs=OptionalLong[1000], topicPartitions=[t-0])',
  );
  assert.equal(
    String(description({ transactionStartTimeMs: null, topicPartitions: null })),
    'TransactionDescription(coordinatorId=1, state=Ongoing, producerId=12345, producerEpoch=15, transactionTimeoutMs=60000, transactionStartTimeMs=OptionalLong.empty, topicPartitions=null)',
  );
  assert.ok(description().equals(description({ topicPartitions: [new TopicPartition({ topic: 't', partition: 0 })] })));
  assert.equal(description().hashCode(), description().hashCode());
  assert.ok(!description().equals(description({ topicPartitions: [] })));
  assert.ok(!description().equals(description({ state: TransactionState.EMPTY })));
});

test('toJSON writes each long as a decimal string', () => {
  assert.deepEqual(JSON.parse(JSON.stringify(description())), {
    coordinatorId: 1,
    state: 'ONGOING',
    producerId: '12345',
    producerEpoch: 15,
    transactionTimeoutMs: '60000',
    transactionStartTimeMs: '1000',
    topicPartitions: [{ topic: 't', partition: 0 }],
  });
});
