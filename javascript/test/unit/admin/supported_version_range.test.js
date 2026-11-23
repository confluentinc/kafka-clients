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

// SupportedVersionRange, which Java ships without tests: the Rust core's
// tests of its translation (supported_version_range.rs), and Java's equals
// and hashCode.

const assert = require('node:assert/strict');

const { FinalizedVersionRange, SupportedVersionRange } = require('@confluentinc/kafka-javascript/admin');

test('new_valid_range', () => {
  const range = new SupportedVersionRange({ minVersion: 1, maxVersion: 5 });
  assert.equal(range.minVersion(), 1);
  assert.equal(range.maxVersion(), 5);
});

test("new_rejects_negative_and_inverted_ranges: Java's message", () => {
  for (const [min, max] of [
    [-1, 2],
    [5, 2],
  ]) {
    assert.throws(() => new SupportedVersionRange({ minVersion: min, maxVersion: max }), {
      name: 'IllegalArgumentError',
      message: `Expected 0 <= minVersion <= maxVersion but received minVersion:${min}, maxVersion:${max}.`,
    });
  }
});

test("to_string_matches_java; equals by Java's getClass, hashCode and toJSON", () => {
  const range = new SupportedVersionRange({ minVersion: 1, maxVersion: 5 });
  assert.equal(String(range), 'SupportedVersionRange[min_version:1, max_version:5]');
  assert.ok(range.equals(new SupportedVersionRange({ minVersion: 1, maxVersion: 5 })));
  assert.ok(!range.equals(new FinalizedVersionRange({ minVersionLevel: 1, maxVersionLevel: 5 })));
  assert.equal(range.hashCode(), 31 * (31 + 1) + 5);
  assert.equal(JSON.stringify(range), '{"minVersion":1,"maxVersion":5}');
});
