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

// Java's OffsetAndMetadataTest, and the binding's own checks of the data
// class. Not translated: testDeserializationCompatibilityBeforeLeaderEpoch
// and testDeserializationCompatibilityWithLeaderEpoch, which read files of
// Java's object serialization, which JavaScript has no counterpart of;
// testSerializationRoundtrip is a JSON round trip, as TopicPartition's.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { OffsetAndMetadata } = require('@confluentinc/kafka-javascript/consumer');

/** @param {import('@confluentinc/kafka-javascript/consumer').OffsetAndMetadata} om */
const roundTrip = (om) => {
  const { offset, leaderEpoch, metadata } = JSON.parse(JSON.stringify(om));
  return new OffsetAndMetadata({ offset: BigInt(offset), leaderEpoch, metadata });
};

test('testInvalidNegativeOffset', () => {
  assert.throws(() => new OffsetAndMetadata({ offset: -239n, leaderEpoch: 15, metadata: '' }), {
    constructor: IllegalArgumentError,
    message: 'Invalid negative offset',
  });
});

test('testSerializationRoundtrip, as a JSON round trip', () => {
  for (const om of [
    new OffsetAndMetadata({ offset: 239n, leaderEpoch: 15, metadata: 'blah' }),
    new OffsetAndMetadata({ offset: 239n, metadata: 'blah' }),
    new OffsetAndMetadata(239n),
  ]) {
    assert.ok(roundTrip(om).equals(om), String(om));
  }
});

test('testEqualsWithNullAndNegativeLeaderEpoch', () => {
  const withNullEpoch = new OffsetAndMetadata({ offset: 100n, leaderEpoch: null, metadata: 'metadata' });
  const withNegativeEpoch = new OffsetAndMetadata({ offset: 100n, leaderEpoch: -1, metadata: 'metadata' });
  assert.ok(withNullEpoch.equals(withNegativeEpoch));
  assert.equal(withNullEpoch.hashCode(), withNegativeEpoch.hashCode());
});

test('testEqualsWithNullAndEmptyMetadata', () => {
  const withNullMetadata = new OffsetAndMetadata({ offset: 100n, leaderEpoch: 1, metadata: null });
  const withEmptyMetadata = new OffsetAndMetadata({ offset: 100n, leaderEpoch: 1, metadata: '' });
  assert.ok(withNullMetadata.equals(withEmptyMetadata));
  assert.equal(withNullMetadata.hashCode(), withEmptyMetadata.hashCode());
});

test("a frozen data object with Java's toString and hashCode", () => {
  const om = new OffsetAndMetadata({ offset: 10, leaderEpoch: 3, metadata: 'm' });
  assert.deepEqual([om.offset, om.leaderEpoch, om.metadata], [10n, 3, 'm']);
  assert.ok(Object.isFrozen(om));
  // Java's Objects.hash(10L, "m", Optional.of(3)), computed by Java.
  assert.equal(om.hashCode(), 42783);
  assert.equal(String(om), "OffsetAndMetadata{offset=10, leaderEpoch=3, metadata='m'}");
  assert.equal(String(new OffsetAndMetadata(10)), "OffsetAndMetadata{offset=10, leaderEpoch=null, metadata=''}");
  assert.equal(JSON.stringify(om), '{"offset":"10","leaderEpoch":3,"metadata":"m"}');
  assert.equal(om.equals({ o: new OffsetAndMetadata({ offset: 10n, leaderEpoch: 3, metadata: 'x' }) }), false);
  assert.equal(om.equals(null), false);
});

test("the constructor takes Java's three forms only", () => {
  assert.equal(new OffsetAndMetadata({ offset: 1, metadata: 'm' }).leaderEpoch, null);
  // @ts-expect-error a leader epoch without metadata: the form check throws IllegalArgumentError
  assert.throws(() => new OffsetAndMetadata({ offset: 1, leaderEpoch: 2 }), {
    constructor: IllegalArgumentError,
    message: 'OffsetAndMetadata() takes one of (offset, leaderEpoch, metadata), (offset, metadata), (offset); got (offset, leaderEpoch)',
  });
  assert.throws(() => new OffsetAndMetadata(1.5), { constructor: TypeError });
  assert.throws(() => new OffsetAndMetadata({ offset: 1, leaderEpoch: 2 ** 31, metadata: '' }), { constructor: RangeError });
  // @ts-expect-error a number for the metadata: the type check throws TypeError
  assert.throws(() => new OffsetAndMetadata({ offset: 1, metadata: 5 }), {
    constructor: TypeError,
    message: 'metadata must be a string or null',
  });
});
