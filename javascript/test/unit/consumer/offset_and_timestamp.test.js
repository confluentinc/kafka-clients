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

// Java has no OffsetAndTimestampTest: the core's offset_and_timestamp.rs
// tests (six), and the binding's own checks of the class.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { OffsetAndTimestamp } = require('@confluentinc/kafka-javascript/consumer');

// OffsetAndTimestamp's constructor as its implementation takes its parameter,
// for the key sets it rejects, which no overload names.
const AnyForm = /** @type {new (options: unknown) => OffsetAndTimestamp} */ (/** @type {unknown} */ (OffsetAndTimestamp));

test('test_accessors', () => {
  const oat = new OffsetAndTimestamp({ offset: 10, timestamp: 100 });
  assert.equal(oat.offset(), 10n);
  assert.equal(oat.timestamp(), 100n);
  assert.equal(oat.leaderEpoch(), null);
});

test('test_with_leader_epoch', () => {
  const oat = new OffsetAndTimestamp({ offset: 10, timestamp: 100, leaderEpoch: 5 });
  assert.equal(oat.leaderEpoch(), 5);
});

test('test_invalid_negative_offset', () => {
  assert.throws(() => new OffsetAndTimestamp({ offset: -1, timestamp: 100 }), {
    constructor: IllegalArgumentError,
    message: 'Invalid negative offset',
  });
});

test('test_invalid_negative_timestamp', () => {
  assert.throws(() => new OffsetAndTimestamp({ offset: 10, timestamp: -1 }), {
    constructor: IllegalArgumentError,
    message: 'Invalid negative timestamp',
  });
});

test('test_display', () => {
  assert.equal(String(new OffsetAndTimestamp({ offset: 10, timestamp: 100, leaderEpoch: 5 })), '(timestamp=100, leaderEpoch=5, offset=10)');
  assert.equal(String(new OffsetAndTimestamp({ offset: 10, timestamp: 100 })), '(timestamp=100, leaderEpoch=null, offset=10)');
});

test('test_equality', () => {
  const a = new OffsetAndTimestamp({ offset: 10, timestamp: 100, leaderEpoch: 5 });
  const b = new OffsetAndTimestamp({ offset: 10, timestamp: 100, leaderEpoch: 5 });
  const c = new OffsetAndTimestamp({ offset: 10, timestamp: 100, leaderEpoch: 6 });
  assert.ok(a.equals(b));
  assert.equal(a.equals(c), false);
});

test("Java's hashCode, the checks in Java's order and toJSON()", () => {
  // Java's Objects.hash(100L, 10L, Optional.of(5)), and with no epoch.
  assert.equal(new OffsetAndTimestamp({ offset: 10n, timestamp: 100n, leaderEpoch: 5 }).hashCode(), 126206);
  assert.equal(new OffsetAndTimestamp({ offset: 10n, timestamp: 100n }).hashCode(), 126201);
  assert.ok(
    new OffsetAndTimestamp({ offset: 1, timestamp: 2, leaderEpoch: null }).equals({
      o: new OffsetAndTimestamp({ offset: 1, timestamp: 2 }),
    }),
  );
  // The offset is checked before the timestamp.
  assert.throws(() => new OffsetAndTimestamp({ offset: -1, timestamp: -1 }), {
    constructor: IllegalArgumentError,
    message: 'Invalid negative offset',
  });
  // @ts-expect-error a wrong argument: the type check throws TypeError
  assert.throws(() => new OffsetAndTimestamp({ offset: '1', timestamp: 2 }), { constructor: TypeError });
  assert.throws(() => new OffsetAndTimestamp({ offset: 1, timestamp: 2, leaderEpoch: 2 ** 31 }), { constructor: RangeError });
  assert.throws(() => new AnyForm({ offset: 1 }), {
    constructor: IllegalArgumentError,
    message: 'OffsetAndTimestamp() takes one of (offset, timestamp), (offset, timestamp, leaderEpoch); got (offset)',
  });
  assert.equal(
    JSON.stringify(new OffsetAndTimestamp({ offset: 10, timestamp: 100, leaderEpoch: 5 })),
    '{"offset":"10","timestamp":"100","leaderEpoch":5}',
  );
  // @ts-expect-error a wrong argument: offset() only reads
  assert.throws(() => new OffsetAndTimestamp({ offset: 1, timestamp: 2 }).offset(1), { constructor: TypeError });
});
