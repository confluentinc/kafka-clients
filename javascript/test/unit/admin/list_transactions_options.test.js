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

// ListTransactionsOptions, which Java ships without tests: its starting
// values, the keyed constructor, Java's copies (a null element kept), Java's
// equals (the timeout left out) and toString.

const assert = require('node:assert/strict');

const { ListTransactionsOptions, TransactionState } = require('@confluentinc/kafka-javascript/admin');

test("Java's starting values", () => {
  const o = new ListTransactionsOptions();
  assert.deepEqual([...o.filteredStates()], []);
  assert.deepEqual([...o.filteredProducerIds()], []);
  assert.equal(o.filteredDuration(), -1n);
  assert.equal(o.filteredTransactionalIdPattern(), null);
  assert.equal(
    String(o),
    'ListTransactionsOptions(filteredStates=[], filteredProducerIds=[], filteredDuration=-1, filteredTransactionalIdPattern=null, timeoutMs=null)',
  );
});

test('the keyed constructor calls each setter; the readers return new sets', () => {
  const o = new ListTransactionsOptions({
    timeoutMs: 100,
    filteredStates: [TransactionState.ONGOING, TransactionState.ONGOING],
    filteredProducerIds: [1n, 2],
    filteredDuration: 5,
    filteredTransactionalIdPattern: 'a.*',
  });
  assert.deepEqual([...o.filteredStates()], [TransactionState.ONGOING]);
  assert.deepEqual([...o.filteredProducerIds()], [1n, 2n]);
  assert.equal(o.filteredDuration(), 5n);
  assert.equal(o.filteredTransactionalIdPattern(), 'a.*');
  o.filteredStates().clear();
  assert.equal(o.filteredStates().size, 1);
  assert.equal(
    String(o),
    'ListTransactionsOptions(filteredStates=[Ongoing], filteredProducerIds=[1, 2], filteredDuration=5, filteredTransactionalIdPattern=a.*, timeoutMs=100)',
  );
});

test("equals leaves the timeout out, as Java's", () => {
  const a = new ListTransactionsOptions({ filteredStates: [TransactionState.EMPTY], timeoutMs: 1 });
  const b = new ListTransactionsOptions({ filteredStates: [TransactionState.EMPTY], timeoutMs: 2 });
  assert.ok(a.equals(b));
  assert.equal(a.hashCode(), b.hashCode());
  assert.ok(!a.equals(new ListTransactionsOptions()));
});

test("the setters' forms and nulls: Java's copy of a null collection throws, a null element is kept", () => {
  const o = new ListTransactionsOptions();
  // @ts-expect-error Java's new HashSet<>(null)
  assert.throws(() => o.filterStates(null), { name: 'TypeError', message: 'states must not be null' });
  // @ts-expect-error Java's new HashSet<>(null)
  assert.throws(() => o.filterProducerIds({ producerIdFilters: null }), {
    name: 'TypeError',
    message: 'producerIdFilters must not be null',
  });
  // @ts-expect-error a null element, which Java's HashSet holds
  o.filterStates([null]);
  assert.deepEqual([...o.filteredStates()], [null]);
  assert.equal(o.filterOnTransactionalIdPattern(null), o);
  assert.throws(() => o.filterOnDuration(1.5), { name: 'TypeError', message: 'durationMs must be a bigint or an integer' });
  // @ts-expect-error the setter takes the value
  assert.throws(() => o.filterStates(), {
    name: 'TypeError',
    message: 'filterStates() takes the value to set: read it with filteredStates()',
  });
});
