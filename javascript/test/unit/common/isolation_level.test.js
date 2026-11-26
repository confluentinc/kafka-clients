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

// IsolationLevel, a Java enum with its id() and forId(), and Java's
// toString() override, the name in lower case. Java has no test of it.

const assert = require('node:assert/strict');

const { IsolationLevel } = require('@confluentinc/kafka-javascript/common');

test("IsolationLevel: the constants, their ids, Java's toString override and toJSON", () => {
  assert.deepEqual(
    IsolationLevel.values().map((l) => [l.name(), l.ordinal(), l.id(), String(l), JSON.stringify(l)]),
    [
      ['READ_UNCOMMITTED', 0, 0, 'read_uncommitted', '"READ_UNCOMMITTED"'],
      ['READ_COMMITTED', 1, 1, 'read_committed', '"READ_COMMITTED"'],
    ],
  );
  assert.equal(IsolationLevel.valueOf('READ_COMMITTED'), IsolationLevel.READ_COMMITTED);
});

test('IsolationLevel.forId: an id no level has is Java\'s "Unknown isolation level"', () => {
  assert.equal(IsolationLevel.forId(1), IsolationLevel.READ_COMMITTED);
  assert.equal(IsolationLevel.forId({ id: 0 }), IsolationLevel.READ_UNCOMMITTED);
  assert.throws(() => IsolationLevel.forId(2), { name: 'IllegalArgumentError', message: 'Unknown isolation level 2' });
  // @ts-expect-error an argument given to a reader: it only reads
  assert.throws(() => IsolationLevel.READ_COMMITTED.id(1), { name: 'TypeError', message: 'id() only reads: it takes no argument' });
});
