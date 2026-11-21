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

// FinalizedVersionRange and SupportedVersionRange, which Java ships without
// tests: the Rust core's tests of its translations
// (finalized_version_range.rs, supported_version_range.rs), and Java's
// equals and hashCode.

const assert = require('node:assert/strict');

const { FinalizedVersionRange } = require('@confluentinc/kafka-javascript/admin');

test('new_valid_range and new_equal_bounds_is_allowed', () => {
  const range = new FinalizedVersionRange({ minVersionLevel: 1, maxVersionLevel: 3 });
  assert.equal(range.minVersionLevel(), 1);
  assert.equal(range.maxVersionLevel(), 3);
  const equal = new FinalizedVersionRange({ minVersionLevel: 2, maxVersionLevel: 2 });
  assert.equal(equal.minVersionLevel(), 2);
  assert.equal(equal.maxVersionLevel(), 2);
});

test("new_rejects_negative_and_inverted_ranges: Java's message", () => {
  for (const [min, max] of [
    [-1, 2],
    [3, 2],
    [0, -1],
  ]) {
    assert.throws(() => new FinalizedVersionRange({ minVersionLevel: min, maxVersionLevel: max }), {
      name: 'IllegalArgumentError',
      message: `Expected minVersionLevel >= 0, maxVersionLevel >= 0 and maxVersionLevel >= minVersionLevel, but received minVersionLevel: ${min}, maxVersionLevel: ${max}`,
    });
  }
});

test('to_string_matches_java; equals, hashCode and toJSON', () => {
  const range = new FinalizedVersionRange({ minVersionLevel: 2, maxVersionLevel: 5 });
  assert.equal(String(range), 'FinalizedVersionRange[min_version_level:2, max_version_level:5]');
  assert.ok(range.equals(new FinalizedVersionRange({ minVersionLevel: 2, maxVersionLevel: 5 })));
  assert.ok(!range.equals(new FinalizedVersionRange({ minVersionLevel: 2, maxVersionLevel: 4 })));
  // Java's Objects.hash(short, short).
  assert.equal(range.hashCode(), 31 * (31 + 2) + 5);
  assert.equal(JSON.stringify(range), '{"minVersionLevel":2,"maxVersionLevel":5}');
});
