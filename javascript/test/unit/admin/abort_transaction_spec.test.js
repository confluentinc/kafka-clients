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

// AbortTransactionSpec, which Java ships without tests: its getters, Java's
// equals and toString, the short epoch's range, and toJSON.

const assert = require('node:assert/strict');

const { AbortTransactionSpec } = require('@confluentinc/kafka-javascript/admin');
const { TopicPartition } = require('@confluentinc/kafka-javascript/common');

const tp = new TopicPartition({ topic: 'foo', partition: 5 });

test("the getters, Java's equals and toString", () => {
  const spec = new AbortTransactionSpec({ topicPartition: tp, producerId: 12345n, producerEpoch: 15, coordinatorEpoch: 3 });
  assert.equal(spec.topicPartition(), tp);
  assert.equal(spec.producerId(), 12345n);
  assert.equal(spec.producerEpoch(), 15);
  assert.equal(spec.coordinatorEpoch(), 3);
  assert.equal(String(spec), 'AbortTransactionSpec(topicPartition=foo-5, producerId=12345, producerEpoch=15, coordinatorEpoch=3)');
  const same = new AbortTransactionSpec({
    topicPartition: new TopicPartition({ topic: 'foo', partition: 5 }),
    producerId: 12345,
    producerEpoch: 15,
    coordinatorEpoch: 3,
  });
  assert.ok(spec.equals(same));
  assert.equal(spec.hashCode(), same.hashCode());
  assert.deepEqual(JSON.parse(JSON.stringify(spec)), {
    topicPartition: { topic: 'foo', partition: 5 },
    producerId: '12345',
    producerEpoch: 15,
    coordinatorEpoch: 3,
  });
});

test('a null partition is stored; the epoch is a Java short', () => {
  const spec = new AbortTransactionSpec({ topicPartition: null, producerId: 1n, producerEpoch: 0, coordinatorEpoch: 0 });
  assert.equal(spec.topicPartition(), null);
  assert.throws(() => new AbortTransactionSpec({ topicPartition: tp, producerId: 1n, producerEpoch: 32768, coordinatorEpoch: 0 }), {
    name: 'RangeError',
    message: 'producerEpoch must be from -32768 to 32767',
  });
});
