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

// ProducerState, which Java ships without tests: its getters, Java's
// equals / hashCode, toString's OptionalInt and OptionalLong forms, and
// toJSON's decimal strings.

const assert = require('node:assert/strict');

const { ProducerState } = require('@confluentinc/kafka-javascript/admin');

/** @param {Partial<ConstructorParameters<typeof ProducerState>[0]>} [over] */
const state = (over = {}) =>
  new ProducerState({
    producerId: 12345n,
    producerEpoch: 15,
    lastSequence: 20,
    lastTimestamp: 1000n,
    coordinatorEpoch: null,
    currentTransactionStartOffset: null,
    ...over,
  });

test('the getters return what the constructor was given; a number is read as a long', () => {
  const s = state({ producerId: 7, coordinatorEpoch: 3, currentTransactionStartOffset: 100n });
  assert.equal(s.producerId(), 7n);
  assert.equal(s.producerEpoch(), 15);
  assert.equal(s.lastSequence(), 20);
  assert.equal(s.lastTimestamp(), 1000n);
  assert.equal(s.coordinatorEpoch(), 3);
  assert.equal(s.currentTransactionStartOffset(), 100n);
  assert.equal(state().coordinatorEpoch(), null);
  assert.equal(state().currentTransactionStartOffset(), null);
});

test("toString writes Java's OptionalInt and OptionalLong", () => {
  assert.equal(
    String(state()),
    'ProducerState(producerId=12345, producerEpoch=15, lastSequence=20, lastTimestamp=1000, coordinatorEpoch=OptionalInt.empty, currentTransactionStartOffset=OptionalLong.empty)',
  );
  assert.equal(
    String(state({ coordinatorEpoch: 2, currentTransactionStartOffset: 50n })),
    'ProducerState(producerId=12345, producerEpoch=15, lastSequence=20, lastTimestamp=1000, coordinatorEpoch=OptionalInt[2], currentTransactionStartOffset=OptionalLong[50])',
  );
});

test("equals and hashCode are Java's", () => {
  assert.ok(state().equals(state()));
  assert.equal(state().hashCode(), state().hashCode());
  assert.ok(!state().equals(state({ coordinatorEpoch: 1 })));
  assert.ok(!state().equals(state({ currentTransactionStartOffset: 1n })));
  assert.ok(!state().equals(null));
  // Java's Objects.hash(12345L, 15, 20, 1000L, OptionalInt.empty(), OptionalLong.empty()):
  // 31-fold over Long.hashCode, Integer.hashCode and 0 for each empty optional.
  assert.equal(state().hashCode(), 2142464139);
});

test('toJSON writes each long as a decimal string', () => {
  assert.deepEqual(JSON.parse(JSON.stringify(state({ currentTransactionStartOffset: 9007199254740993n }))), {
    producerId: '12345',
    producerEpoch: 15,
    lastSequence: 20,
    lastTimestamp: '1000',
    coordinatorEpoch: null,
    currentTransactionStartOffset: '9007199254740993',
  });
});

test("the constructor's form and types", () => {
  // @ts-expect-error a missing key
  assert.throws(() => new ProducerState({ producerId: 1n }), {
    name: 'IllegalArgumentError',
    message:
      'ProducerState() takes one of (producerId, producerEpoch, lastSequence, lastTimestamp, coordinatorEpoch, currentTransactionStartOffset); got (producerId)',
  });
  // An int beyond 32 bits: only the run time refuses it.
  assert.throws(() => state({ producerEpoch: 2 ** 31 }), {
    name: 'RangeError',
    message: 'producerEpoch must be from -2147483648 to 2147483647',
  });
  // @ts-expect-error a reader takes no argument
  assert.throws(() => state().producerId(1), { name: 'TypeError', message: 'producerId() only reads: it takes no argument' });
});
