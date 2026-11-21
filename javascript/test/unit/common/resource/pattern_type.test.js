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

// PatternType, which Java ships without a test: its codes, Java's fromString
// (an exact name, UNKNOWN otherwise, a null one included, as Java's map lookup
// answers) and isSpecific.

const assert = require('node:assert/strict');

const { PatternType } = require('@confluentinc/kafka-javascript/common/resource');

test('the codes, in Java declaration order', () => {
  assert.deepEqual(
    PatternType.values().map((t) => [t.name(), t.code(), t.ordinal()]),
    [
      ['UNKNOWN', 0, 0],
      ['ANY', 1, 1],
      ['MATCH', 2, 2],
      ['LITERAL', 3, 3],
      ['PREFIXED', 4, 4],
    ],
  );
  for (const t of PatternType.values()) assert.equal(PatternType.fromCode(t.code()), t);
  assert.equal(PatternType.fromCode(120), PatternType.UNKNOWN);
});

test('fromString reads the exact name, and UNKNOWN for anything else, null included', () => {
  assert.equal(PatternType.fromString('LITERAL'), PatternType.LITERAL);
  assert.equal(PatternType.fromString({ name: 'PREFIXED' }), PatternType.PREFIXED);
  assert.equal(PatternType.fromString('literal'), PatternType.UNKNOWN);
  assert.equal(PatternType.fromString(null), PatternType.UNKNOWN);
});

test('isSpecific and isUnknown', () => {
  assert.deepEqual(
    PatternType.values().map((t) => [t.name(), t.isSpecific(), t.isUnknown()]),
    [
      ['UNKNOWN', false, true],
      ['ANY', false, false],
      ['MATCH', false, false],
      ['LITERAL', true, false],
      ['PREFIXED', true, false],
    ],
  );
  // @ts-expect-error isSpecific() only reads
  assert.throws(() => PatternType.LITERAL.isSpecific(1), { name: 'TypeError', message: 'isSpecific() only reads: it takes no argument' });
  assert.equal(JSON.stringify(PatternType.MATCH), '"MATCH"');
});
