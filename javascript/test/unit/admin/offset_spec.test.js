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

// OffsetSpec, a Java class hierarchy: its factories make Java's nested
// specs, and TimestampSpec's constructor is not public. Java has no test of
// it: these check Java's factories, as the Python client's do.

const assert = require('node:assert/strict');

const { OffsetSpec } = require('@confluentinc/kafka-javascript/admin');

test("OffsetSpec's factories make Java's nested specs, each a new one", () => {
  assert.ok(OffsetSpec.latest() instanceof OffsetSpec.LatestSpec);
  assert.ok(OffsetSpec.earliest() instanceof OffsetSpec.EarliestSpec);
  assert.ok(OffsetSpec.maxTimestamp() instanceof OffsetSpec.MaxTimestampSpec);
  assert.ok(OffsetSpec.earliestLocal() instanceof OffsetSpec.EarliestLocalSpec);
  assert.ok(OffsetSpec.latestTiered() instanceof OffsetSpec.LatestTieredSpec);
  assert.ok(OffsetSpec.earliestPendingUpload() instanceof OffsetSpec.EarliestPendingUploadSpec);
  assert.ok(OffsetSpec.forTimestamp(5) instanceof OffsetSpec.TimestampSpec);
  assert.ok(OffsetSpec.forTimestamp({ timestamp: 5n }) instanceof OffsetSpec);
  assert.notEqual(OffsetSpec.latest(), OffsetSpec.latest());
  // Java's public constructors.
  assert.ok(new OffsetSpec() instanceof OffsetSpec);
  assert.ok(new OffsetSpec.EarliestSpec() instanceof OffsetSpec);
  // A constructor of no parameter refuses an argument with the form error.
  // @ts-expect-error a wrong argument: Java's constructor takes none
  assert.throws(() => new OffsetSpec(1), { name: 'IllegalArgumentError', message: 'OffsetSpec() takes one of (); got a number' });
  // @ts-expect-error a wrong argument: the nested specs inherit it
  assert.throws(() => new OffsetSpec.EarliestSpec(1), {
    name: 'IllegalArgumentError',
    message: 'OffsetSpec() takes one of (); got a number',
  });
});

test('OffsetSpec.forTimestamp takes a Java long; the factories of no parameter refuse an argument', () => {
  assert.throws(() => OffsetSpec.forTimestamp(2 ** 53), {
    name: 'RangeError',
    message: 'timestamp must be a bigint beyond 2^53 - 1: a number holds no larger integer exactly',
  });
  // @ts-expect-error a wrong argument: a timestamp is a bigint or a whole number
  assert.throws(() => OffsetSpec.forTimestamp({ timestamp: '5' }), {
    name: 'TypeError',
    message: 'timestamp must be a bigint or an integer',
  });
  // @ts-expect-error a wrong argument: latest() takes none
  assert.throws(() => OffsetSpec.latest(1), { name: 'TypeError', message: 'latest() only reads: it takes no argument' });
});
