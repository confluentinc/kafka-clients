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

// ElectionType, a Java enum with the public field `value` and Java's static
// valueOf(byte) beside Enum's valueOf(String), one method. Java has no test of
// it: these check Java's members, as python/confluent_kafka/common/election_type.py.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { ElectionType } = require('@confluentinc/kafka-javascript/common');

test("ElectionType: the constants, their value, Enum's members and toJSON", () => {
  assert.deepEqual(ElectionType.values(), [ElectionType.PREFERRED, ElectionType.UNCLEAN]);
  assert.deepEqual(
    ElectionType.values().map((t) => [t.name(), t.ordinal(), t.value, String(t), JSON.stringify(t)]),
    [
      ['PREFERRED', 0, 0, 'PREFERRED', '"PREFERRED"'],
      ['UNCLEAN', 1, 1, 'UNCLEAN', '"UNCLEAN"'],
    ],
  );
  assert.ok(Object.isFrozen(ElectionType.PREFERRED));
});

test('ElectionType.valueOf: by name or by value, bare or named', () => {
  assert.equal(ElectionType.valueOf('UNCLEAN'), ElectionType.UNCLEAN);
  assert.equal(ElectionType.valueOf({ name: 'PREFERRED' }), ElectionType.PREFERRED);
  assert.equal(ElectionType.valueOf(1), ElectionType.UNCLEAN);
  assert.equal(ElectionType.valueOf({ value: 0 }), ElectionType.PREFERRED);
  assert.throws(() => ElectionType.valueOf(5), { name: 'IllegalArgumentError', message: 'Value 5 must be one of [PREFERRED, UNCLEAN]' });
  assert.throws(
    () => ElectionType.valueOf('X'),
    (e) => e instanceof IllegalArgumentError && e.message === 'No enum constant org.apache.kafka.common.ElectionType.X',
  );
  // @ts-expect-error a null the types forbid: java.lang.Enum.valueOf's requireNonNull
  assert.throws(() => ElectionType.valueOf(null), { name: 'TypeError', message: 'Name is null' });
  assert.throws(() => ElectionType.valueOf(200), { name: 'RangeError', message: 'value must be from -128 to 127' });
});
