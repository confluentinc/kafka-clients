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

// TransactionState, an enum Java ships without tests: java.lang.Enum's
// members, Java's display names, and Java's parse (getOrDefault on the
// display names, UNKNOWN for anything else, null included).

const assert = require('node:assert/strict');

const { TransactionState } = require('@confluentinc/kafka-javascript/admin');

const JAVA = [
  ['ONGOING', 'Ongoing'],
  ['PREPARE_ABORT', 'PrepareAbort'],
  ['PREPARE_COMMIT', 'PrepareCommit'],
  ['COMPLETE_ABORT', 'CompleteAbort'],
  ['COMPLETE_COMMIT', 'CompleteCommit'],
  ['EMPTY', 'Empty'],
  ['PREPARE_EPOCH_FENCE', 'PrepareEpochFence'],
  ['UNKNOWN', 'Unknown'],
];

test("the constants in Java's order, with Java's names and display names", () => {
  const values = TransactionState.values();
  assert.deepEqual(
    values.map((s) => [s.name(), s.toString()]),
    JAVA,
  );
  values.forEach((s, i) => {
    assert.equal(s.ordinal(), i);
    assert.equal(TransactionState.valueOf(s.name()), s);
    assert.equal(JSON.stringify(s), JSON.stringify(s.name()));
    assert.ok(Object.isFrozen(s));
  });
});

test("parse matches the display name exactly, else UNKNOWN, as Java's getOrDefault", () => {
  assert.equal(TransactionState.parse('Ongoing'), TransactionState.ONGOING);
  assert.equal(TransactionState.parse({ name: 'PrepareEpochFence' }), TransactionState.PREPARE_EPOCH_FENCE);
  assert.equal(TransactionState.parse('ongoing'), TransactionState.UNKNOWN);
  assert.equal(TransactionState.parse('ONGOING'), TransactionState.UNKNOWN);
  assert.equal(TransactionState.parse(null), TransactionState.UNKNOWN);
});

test("valueOf's errors are Java's", () => {
  assert.throws(() => TransactionState.valueOf('Ongoing'), {
    name: 'IllegalArgumentError',
    message: 'No enum constant org.apache.kafka.clients.admin.TransactionState.Ongoing',
  });
  // @ts-expect-error Java's null name
  assert.throws(() => TransactionState.valueOf(null), { name: 'TypeError', message: 'Name is null' });
});
