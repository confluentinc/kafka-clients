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

// Java's ConsumerRecordTest (both tests), and the binding's own checks of the
// data class: its three forms, the headers it takes, the errors in Java's
// order, toString(), toJSON() and the frozen object.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { ConsumerRecord } = require('@confluentinc/kafka-javascript/consumer');
const { Headers, RecordHeader } = require('@confluentinc/kafka-javascript/common/header');
const { TimestampType } = require('@confluentinc/kafka-javascript/common/record');

// ConsumerRecord's constructor as its implementation takes its parameter, for
// the key sets it rejects, which no overload names.
const AnyForm = /** @type {new (options: unknown) => ConsumerRecord<unknown, unknown>} */ (/** @type {unknown} */ (ConsumerRecord));

/** The long form's fields, but the headers. */
const LONG = {
  topic: 'topic',
  partition: 0,
  offset: 23n,
  timestamp: 23434217432432n,
  timestampType: TimestampType.CREATE_TIME,
  serializedKeySize: 100,
  serializedValueSize: 1142,
  key: 'key',
  value: 'value',
  leaderEpoch: null,
};

test('testShortConstructor', () => {
  const topic = 'topic';
  const partition = 0;
  const offset = 23n;
  const key = 'key';
  const value = 'value';

  const record = new ConsumerRecord({ topic, partition, offset, key, value });
  assert.equal(record.topic, topic);
  assert.equal(record.partition, partition);
  assert.equal(record.offset, offset);
  assert.equal(record.key, key);
  assert.equal(record.value, value);
  assert.equal(record.timestampType, TimestampType.NO_TIMESTAMP_TYPE);
  assert.equal(record.timestamp, ConsumerRecord.NO_TIMESTAMP);
  assert.equal(record.serializedKeySize, ConsumerRecord.NULL_SIZE);
  assert.equal(record.serializedValueSize, ConsumerRecord.NULL_SIZE);
  assert.equal(record.leaderEpoch, null);
  assert.equal(record.deliveryCount, null);
  assert.ok(record.headers.equals(new Headers()));
});

test('testLongConstructor', () => {
  const headers = new Headers();
  headers.add(new RecordHeader({ key: 'header key', value: Buffer.from('header value', 'utf8') }));
  let record = new ConsumerRecord({ ...LONG, headers });
  assert.equal(record.topic, LONG.topic);
  assert.equal(record.partition, LONG.partition);
  assert.equal(record.offset, LONG.offset);
  assert.equal(record.key, LONG.key);
  assert.equal(record.value, LONG.value);
  assert.equal(record.timestampType, LONG.timestampType);
  assert.equal(record.timestamp, LONG.timestamp);
  assert.equal(record.serializedKeySize, LONG.serializedKeySize);
  assert.equal(record.serializedValueSize, LONG.serializedValueSize);
  assert.equal(record.leaderEpoch, null);
  assert.equal(record.deliveryCount, null);
  assert.ok(record.headers.equals(headers));

  const leaderEpoch = 10;
  const deliveryCount = 1;
  record = new ConsumerRecord({ ...LONG, headers, leaderEpoch, deliveryCount });
  assert.equal(record.topic, LONG.topic);
  assert.equal(record.partition, LONG.partition);
  assert.equal(record.offset, LONG.offset);
  assert.equal(record.key, LONG.key);
  assert.equal(record.value, LONG.value);
  assert.equal(record.timestampType, LONG.timestampType);
  assert.equal(record.timestamp, LONG.timestamp);
  assert.equal(record.serializedKeySize, LONG.serializedKeySize);
  assert.equal(record.serializedValueSize, LONG.serializedValueSize);
  assert.equal(record.leaderEpoch, leaderEpoch);
  assert.equal(record.deliveryCount, deliveryCount);
  assert.ok(record.headers.equals(headers));
});

test('a Headers is kept; an iterable makes a new Headers holding the same Header objects', () => {
  const headers = new Headers();
  assert.equal(new ConsumerRecord({ ...LONG, headers }).headers, headers);
  const header = new RecordHeader({ key: 'a', value: Buffer.from([1]) });
  const record = new ConsumerRecord({ ...LONG, headers: [header, ['b', null]] });
  assert.ok(record.headers instanceof Headers);
  const [first, second] = record.headers.toArray();
  assert.equal(first, header);
  assert.ok(second instanceof RecordHeader);
  assert.equal(second.key(), 'b');
  assert.equal(second.value(), null);
  // The record's headers stay writable, as Java's are.
  record.headers.add({ key: 'c', value: null });
  assert.equal(record.headers.toArray().length, 3);
});

test("the errors, in Java's order", () => {
  // The argument types first, then the caller's `new RecordHeaders(...)` and
  // `new RecordHeader(...)`, then the constructor's own checks.
  // @ts-expect-error a wrong argument: the type check throws TypeError
  assert.throws(() => new ConsumerRecord({ ...LONG, topic: 5, headers: null }), {
    constructor: TypeError,
    message: 'topic must be a string',
  });
  // @ts-expect-error a null the types forbid: Java's constructor checks the topic
  assert.throws(() => new ConsumerRecord({ ...LONG, topic: null, partition: 1.5, headers: null }), {
    constructor: TypeError,
    message: 'partition must be an integer',
  });
  // @ts-expect-error a null the types forbid: Java's constructor checks the topic
  assert.throws(() => new ConsumerRecord({ ...LONG, topic: null, headers: [null] }), {
    constructor: TypeError,
    message: 'Header cannot be null.',
  });
  // @ts-expect-error a null the types forbid: Java's constructor checks the topic
  assert.throws(() => new ConsumerRecord({ ...LONG, topic: null, headers: null }), {
    constructor: IllegalArgumentError,
    message: 'Topic cannot be null',
  });
  // @ts-expect-error a null the types forbid: Java's constructor checks the headers
  assert.throws(() => new ConsumerRecord({ ...LONG, headers: null }), {
    constructor: IllegalArgumentError,
    message: 'Headers cannot be null',
  });
  // @ts-expect-error a null the types forbid: Java's constructor checks the topic
  assert.throws(() => new ConsumerRecord({ topic: null, partition: 0, offset: 0, key: null, value: null }), {
    constructor: IllegalArgumentError,
    message: 'Topic cannot be null',
  });
  // @ts-expect-error a wrong argument: the type check throws TypeError
  assert.throws(() => new ConsumerRecord({ ...LONG, headers: 'h' }), { constructor: TypeError });
  // @ts-expect-error a wrong argument: the type check throws TypeError
  assert.throws(() => new ConsumerRecord({ ...LONG, headers: null, timestampType: 'CreateTime' }), {
    constructor: TypeError,
    message: 'timestampType must be a TimestampType',
  });
  assert.throws(() => new ConsumerRecord({ ...LONG, headers: [], deliveryCount: 2 ** 15 }), {
    constructor: RangeError,
    message: 'deliveryCount must be from -32768 to 32767',
  });
  assert.throws(() => new ConsumerRecord({ ...LONG, headers: [], offset: 2 ** 53 }), { constructor: RangeError });
});

test('the three forms, matched exactly', () => {
  const short = new ConsumerRecord({ topic: 't', partition: 1, offset: 2, key: null, value: null });
  assert.equal(short.offset, 2n);
  const long = new ConsumerRecord({ ...LONG, headers: [], leaderEpoch: 4 });
  assert.equal(long.leaderEpoch, 4);
  assert.equal(long.deliveryCount, null);
  const longest = new ConsumerRecord({ ...LONG, headers: [], deliveryCount: 3 });
  assert.equal(longest.deliveryCount, 3);
  const forms =
    'ConsumerRecord() takes one of (topic, partition, offset, key, value), ' +
    '(topic, partition, offset, timestamp, timestampType, serializedKeySize, serializedValueSize, key, value, headers, leaderEpoch), ' +
    '(topic, partition, offset, timestamp, timestampType, serializedKeySize, serializedValueSize, key, value, headers, leaderEpoch, deliveryCount)';
  // Nothing is filled in: a record with headers needs every field of the long form.
  assert.throws(() => new AnyForm({ topic: 't', partition: 1, offset: 2, key: null, value: null, headers: [] }), {
    constructor: IllegalArgumentError,
    message: `${forms}; got (topic, partition, offset, key, value, headers)`,
  });
  assert.throws(() => new AnyForm({ topic: 't', partition: 1, offset: 2, value: null }), {
    constructor: IllegalArgumentError,
    message: `${forms}; got (topic, partition, offset, value)`,
  });
  assert.throws(() => new AnyForm({ topic: 't', partition: 1, offset: 2, key: undefined, value: null }), {
    constructor: TypeError,
    message: "key must not be undefined. Leave the key out, or pass null for Java's null",
  });
});

test("toString() is Java's", () => {
  const headers = [new RecordHeader({ key: 'h', value: Buffer.from([1, 255]) })];
  const record = new ConsumerRecord({ ...LONG, key: Buffer.from([2]), value: 'v', headers, leaderEpoch: 7, deliveryCount: 2 });
  assert.equal(
    String(record),
    'ConsumerRecord(topic = topic, partition = 0, leaderEpoch = 7, offset = 23, CreateTime = 23434217432432, ' +
      'deliveryCount = 2, serialized key size = 100, serialized value size = 1142, ' +
      'headers = RecordHeaders(headers = [RecordHeader(key = h, value = [1, -1])], isReadOnly = false), key = [2], value = v)',
  );
  assert.equal(
    String(new ConsumerRecord({ topic: 't', partition: 1, offset: 2, key: null, value: null })),
    'ConsumerRecord(topic = t, partition = 1, leaderEpoch = null, offset = 2, NoTimestampType = -1, ' +
      'deliveryCount = null, serialized key size = -1, serialized value size = -1, ' +
      'headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = null)',
  );
});

test('toJSON() writes the longs as decimal strings; the record is frozen', () => {
  const record = new ConsumerRecord({ ...LONG, headers: [['h', null]], leaderEpoch: 7 });
  assert.equal(
    JSON.stringify(record),
    '{"topic":"topic","partition":0,"offset":"23","timestamp":"23434217432432","timestampType":"CREATE_TIME",' +
      '"serializedKeySize":100,"serializedValueSize":1142,"headers":[{"key":"h","value":null}],"key":"key","value":"value",' +
      '"leaderEpoch":7,"deliveryCount":null}',
  );
  assert.ok(Object.isFrozen(record));
  assert.throws(() => {
    // @ts-expect-error a wrong argument: the field is readonly, and the frozen object throws TypeError
    record.topic = 'x';
  }, TypeError);
});
