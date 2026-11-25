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

// Java's UuidTest, then the binding's own: the forms, the JSON form and
// Java's messages for an input fromString refuses.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { Uuid, UuidSet } = require('@confluentinc/kafka-javascript/common');

test('testSignificantBits', () => {
  const id = new Uuid({ mostSigBits: 34n, leastSigBits: 98n });
  assert.equal(id.getMostSignificantBits(), 34n);
  assert.equal(id.getLeastSignificantBits(), 98n);
});

test('testUuidEquality', () => {
  const id1 = new Uuid({ mostSigBits: 12n, leastSigBits: 13n });
  const id2 = new Uuid({ mostSigBits: 12n, leastSigBits: 13n });
  const id3 = new Uuid({ mostSigBits: 24n, leastSigBits: 38n });
  assert.ok(Uuid.ZERO_UUID.equals(Uuid.ZERO_UUID));
  assert.ok(id1.equals(id2));
  assert.ok(!id1.equals(id3));
  assert.equal(Uuid.ZERO_UUID.hashCode(), Uuid.ZERO_UUID.hashCode());
  assert.equal(id1.hashCode(), id2.hashCode());
  assert.notEqual(id1.hashCode(), id3.hashCode());
});

test('testHashCode', () => {
  assert.equal(new Uuid({ mostSigBits: 16n, leastSigBits: 7n }).hashCode(), 23);
  assert.equal(new Uuid({ mostSigBits: 1043n, leastSigBits: 20075n }).hashCode(), 19064);
  assert.equal(new Uuid({ mostSigBits: 104312423523523n, leastSigBits: 200732425676585n }).hashCode(), -2011255899);
});

test('testStringConversion', () => {
  const id = Uuid.randomUuid();
  assert.ok(Uuid.fromString(id.toString()).equals(id));
  assert.ok(Uuid.ZERO_UUID.equals(Uuid.fromString(Uuid.ZERO_UUID.toString())));
});

test('testRandomUuid, repeated 100 times', () => {
  for (let i = 0; i < 100; i++) {
    const randomID = Uuid.randomUuid();
    assert.ok(!randomID.equals(Uuid.ZERO_UUID));
    assert.ok(!randomID.equals(Uuid.METADATA_TOPIC_ID));
    assert.ok(!randomID.toString().includes('-'));
  }
});

test('testCompareUuids', () => {
  const id00 = new Uuid({ mostSigBits: 0n, leastSigBits: 0n });
  const id01 = new Uuid({ mostSigBits: 0n, leastSigBits: 1n });
  const id10 = new Uuid({ mostSigBits: 1n, leastSigBits: 0n });
  assert.equal(id00.compareTo(id00), 0);
  assert.equal(id01.compareTo(id01), 0);
  assert.equal(id10.compareTo(id10), 0);
  assert.equal(id00.compareTo(id01), -1);
  assert.equal(id00.compareTo(id10), -1);
  assert.equal(id01.compareTo(id00), 1);
  assert.equal(id10.compareTo(id00), 1);
  assert.equal(id01.compareTo(id10), -1);
  assert.equal(id10.compareTo(id01), 1);
});

test('testFromStringWithInvalidInput', () => {
  const oversizeString = Buffer.alloc(32).toString('base64url');
  assert.throws(() => Uuid.fromString(oversizeString), {
    name: 'IllegalArgumentError',
    message: 'Input string with prefix `AAAAAAAAAAAAAAAAAAAAAAAA` is too long to be decoded as a base64 UUID',
  });
  const undersizeString = Buffer.alloc(4).toString('base64url');
  assert.throws(() => Uuid.fromString(undersizeString), {
    name: 'IllegalArgumentError',
    message: 'Input string `AAAAAA` decoded as 4 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID',
  });
});

test('testToArray', () => {
  assert.equal(Uuid.toArray(null), null);
  const other = Uuid.fromString('UXyU9i5ARn6W00ON2taeWA');
  const array = Uuid.toArray([Uuid.ZERO_UUID, other]);
  assert.ok(array);
  assert.equal(array.length, 2);
  assert.ok(array[0].equals(Uuid.ZERO_UUID));
  assert.ok(array[1].equals(other));
  // Java's List: any iterable, copied.
  assert.deepEqual(Uuid.toArray(new Set([other])), [other]);
  assert.deepEqual(
    Uuid.toArray({
      list: (function* () {
        yield other;
      })(),
    }),
    [other],
  );
});

test('testToList', () => {
  assert.equal(Uuid.toList(null), null);
  const other = Uuid.fromString('UXyU9i5ARn6W00ON2taeWA');
  const list = Uuid.toList([Uuid.ZERO_UUID, other]);
  assert.ok(list);
  assert.deepEqual(
    list.map((u) => u.toString()),
    [Uuid.ZERO_UUID.toString(), 'UXyU9i5ARn6W00ON2taeWA'],
  );
});

test("fromString refuses what Java's base64 decoder refuses, with its messages", () => {
  assert.throws(() => Uuid.fromString('AAAAAAAAAAAAAAAAAAAA+A'), { name: 'IllegalArgumentError', message: 'Illegal base64 character 2b' });
  assert.throws(() => Uuid.fromString('A'), { name: 'IllegalArgumentError', message: 'Last unit does not have enough valid bits' });
  // @ts-expect-error a null string: Java dereferences it, a TypeError
  assert.throws(() => Uuid.fromString(null), { name: 'TypeError', message: 'str must not be null' });
  assert.ok(Uuid.fromString({ str: 'UXyU9i5ARn6W00ON2taeWA' }).equals(Uuid.fromString('UXyU9i5ARn6W00ON2taeWA')));
});

test("RESERVED holds its members by value, as Java's Set.of compares them with equals, and is read-only", () => {
  assert.equal(Uuid.RESERVED.has(new Uuid({ mostSigBits: 0n, leastSigBits: 0n })), true);
  assert.equal(Uuid.RESERVED.has(Uuid.fromString(Uuid.ZERO_UUID.toString())), true);
  assert.equal(Uuid.RESERVED.has(new Uuid({ mostSigBits: 0n, leastSigBits: 1n })), true);
  assert.equal(Uuid.RESERVED.has(new Uuid({ mostSigBits: 0n, leastSigBits: 2n })), false);
  assert.equal(Uuid.RESERVED, Uuid.RESERVED);
  assert.ok(Uuid.RESERVED instanceof UuidSet);
  for (const write of [
    () => Uuid.RESERVED.add(Uuid.randomUuid()),
    () => Uuid.RESERVED.delete(Uuid.ZERO_UUID),
    () => Uuid.RESERVED.clear(),
  ]) {
    assert.throws(write, { name: 'UnsupportedOperationError', message: '' });
  }
  assert.equal(Uuid.RESERVED.size(), 2);
  assert.equal(Uuid.RESERVED.toString(), '[AAAAAAAAAAAAAAAAAAAAAA, AAAAAAAAAAAAAAAAAAAAAQ]');
});

test('the constants, the long halves and the JSON form', () => {
  assert.equal(Uuid.ONE_UUID.toString(), 'AAAAAAAAAAAAAAAAAAAAAQ');
  assert.equal(Uuid.METADATA_TOPIC_ID, Uuid.ONE_UUID);
  assert.ok(Uuid.RESERVED.has(Uuid.ZERO_UUID) && Uuid.RESERVED.has(Uuid.ONE_UUID));
  assert.equal(Uuid.RESERVED.size(), 2);
  const negative = new Uuid({ mostSigBits: -1n, leastSigBits: -(2n ** 63n) });
  assert.equal(negative.getMostSignificantBits(), -1n);
  assert.ok(Uuid.fromString(negative.toString()).equals(negative));
  assert.equal(JSON.stringify({ id: Uuid.ONE_UUID }), '{"id":"AAAAAAAAAAAAAAAAAAAAAQ"}');
  assert.throws(() => new Uuid({ mostSigBits: 2n ** 63n, leastSigBits: 0n }), RangeError);
  // @ts-expect-error a missing half: the form check throws IllegalArgumentError
  assert.throws(() => new Uuid({ mostSigBits: 1n }), IllegalArgumentError);
  assert.ok(!Uuid.ZERO_UUID.equals(null));
});
