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

// MockConsumerTest (Apache Kafka 4.4.0), then the binding's own contracts of
// MockConsumer, which wraps the FFI's mock (javascript/CLAUDE.md, JavaScript
// Binding Conventions, Implementation over the FFI). Each case awaits where
// Java blocks and closes its mock, which keeps Node running until close().
// The messages are Java's, the core's where they differ, each said so.
//
// All of Java's 13 tests are ported here: testSimpleMock,
// testConsumerRecordsIsEmptyWhenReturningNoRecords,
// shouldNotClearRecordsForPausedPartitions, endOffsetsShouldBeIdempotent,
// testDurationBasedOffsetReset, testRebalanceListener,
// testRe2JPatternSubscription, shouldReturnMaxPollRecords,
// testLosePartitionsCallsOnPartitionsLost,
// testLosePartitionsRemovesFromAssignment,
// testLosePartitionsThrowsIfNotAssigned,
// testLosePartitionsClearsOnlyLostRecords and testLosePartitionsThenRebalance.

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const {
  ConcurrentModificationError,
  IllegalArgumentError,
  IllegalStateError,
  UnsupportedOperationError,
} = require('@confluentinc/kafka-javascript');
const { KafkaError, Node, PartitionInfo, TopicPartition, TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');
const { Headers, RecordHeader } = require('@confluentinc/kafka-javascript/common/header');
const { TimestampType } = require('@confluentinc/kafka-javascript/common/record');
const {
  CloseOptions,
  ConsumerGroupMetadata,
  ConsumerRecord,
  MockConsumer,
  NoOffsetForPartitionError,
  OffsetAndMetadata,
  SubscriptionPattern,
} = require('@confluentinc/kafka-javascript/consumer');

const CLOSED = 'This consumer has already been closed.';
const CONCURRENT = 'KafkaConsumer is not safe for multi-threaded access.';
const TP0 = new TopicPartition({ topic: 'test', partition: 0 });
const TP1 = new TopicPartition({ topic: 'test', partition: 1 });

/** @type {MockConsumer<string, string>[]} */
const opened = [];

/**
 * A mock closed after the test.
 *
 * @param {string} [strategy] the offset reset strategy
 * @returns {MockConsumer<string, string>} the mock
 */
function newMock(strategy = 'earliest') {
  /** @type {MockConsumer<string, string>} */
  const mock = new MockConsumer(strategy);
  opened.push(mock);
  return mock;
}

afterEach(async () => {
  for (const mock of opened.splice(0)) await mock.close();
});

/**
 * A record of `test` with only Java's five-parameter fields.
 *
 * @param {number} partition
 * @param {number} offset
 * @returns {ConsumerRecord<string, string>} the record
 */
const record = (partition, offset) => new ConsumerRecord({ topic: 'test', partition, offset, key: null, value: null });

/**
 * Java's eleven-parameter record of `test`, partition 0, with no leader epoch.
 *
 * @param {number} offset
 * @param {string} key
 * @param {string} value
 * @returns {ConsumerRecord<string, string>} the record
 */
function fullRecord(offset, key, value) {
  return new ConsumerRecord({
    topic: 'test',
    partition: 0,
    offset,
    timestamp: 0,
    timestampType: TimestampType.CREATE_TIME,
    serializedKeySize: 0,
    serializedValueSize: 0,
    key,
    value,
    headers: new Headers(),
    leaderEpoch: null,
  });
}

/**
 * A map of partition to offset.
 *
 * @param {Array<[TopicPartition, bigint | number]>} entries
 * @returns {TopicPartitionMap<bigint | number>} the map
 */
const offsets = (entries) => new TopicPartitionMap(entries);

// ---- MockConsumerTest -------------------------------------------------------

test('testSimpleMock', async () => {
  const consumer = newMock('earliest');
  await consumer.subscribe(['test']);
  assert.equal((await consumer.poll(0)).count(), 0);
  await consumer.rebalance([TP0, TP1]);
  // Mock consumers need to seek manually since they cannot automatically reset offsets
  consumer.updateBeginningOffsets(
    offsets([
      [TP0, 0n],
      [TP1, 0n],
    ]),
  );
  await consumer.seek({ partition: TP0, offset: 0 });
  const rec1 = fullRecord(0, 'key1', 'value1');
  const rec2 = fullRecord(1, 'key2', 'value2');
  consumer.addRecord(rec1);
  consumer.addRecord(rec2);
  const recs = await consumer.poll(1);
  const iter = recs[Symbol.iterator]();
  // Java's assertEquals of a ConsumerRecord, which has no equals(): identity.
  assert.equal(iter.next().value, rec1);
  assert.equal(iter.next().value, rec2);
  assert.ok(iter.next().done);
  assert.equal(await consumer.position(TP0), 2n);
  assert.equal(recs.nextOffsets().size(), 1);
  assert.ok(new OffsetAndMetadata({ offset: 2, leaderEpoch: null, metadata: '' }).equals(recs.nextOffsets().get(TP0)));
  await consumer.commitSync();
  assert.equal((await consumer.committed([TP0])).get(TP0)?.offset, 2n);
});

test('testConsumerRecordsIsEmptyWhenReturningNoRecords', async () => {
  const consumer = newMock();
  await consumer.assign([TP0]);
  consumer.addRecord(record(0, 0));
  consumer.updateEndOffsets(offsets([[TP0, 1n]]));
  await consumer.seekToEnd([TP0]);
  const records = await consumer.poll(1);
  assert.equal(records.count(), 0);
  assert.ok(records.isEmpty());
});

test('shouldNotClearRecordsForPausedPartitions', async () => {
  const consumer = newMock();
  const testPartitionList = [TP0];
  await consumer.assign(testPartitionList);
  consumer.addRecord(record(0, 0));
  consumer.updateBeginningOffsets(offsets([[TP0, 0n]]));
  await consumer.seekToBeginning(testPartitionList);

  await consumer.pause(testPartitionList);
  await consumer.poll(1);
  await consumer.resume(testPartitionList);
  const recordsSecondPoll = await consumer.poll(1);
  assert.equal(recordsSecondPoll.count(), 1);
  assert.equal(recordsSecondPoll.nextOffsets().size(), 1);
  assert.ok(new OffsetAndMetadata({ offset: 1, leaderEpoch: null, metadata: '' }).equals(recordsSecondPoll.nextOffsets().get(TP0)));
});

test('endOffsetsShouldBeIdempotent', async () => {
  const consumer = newMock();
  consumer.updateEndOffsets(offsets([[TP0, 10n]]));
  // consumer.endOffsets should NOT change the value of end offsets
  for (let i = 0; i < 3; i++) assert.equal((await consumer.endOffsets([TP0])).get(TP0), 10n);
  consumer.updateEndOffsets(offsets([[TP0, 11n]]));
  // consumer.endOffsets should NOT change the value of end offsets
  for (let i = 0; i < 3; i++) assert.equal((await consumer.endOffsets([TP0])).get(TP0), 11n);
});

test('testDurationBasedOffsetReset', async () => {
  const consumer = newMock('by_duration:PT1H');
  await consumer.subscribe(['test']);
  await consumer.rebalance([TP0, TP1]);
  consumer.updateDurationOffsets(
    offsets([
      [TP0, 10n],
      [TP1, 11n],
    ]),
  );
  const rec1 = fullRecord(10, 'key1', 'value1');
  const rec2 = fullRecord(11, 'key2', 'value2');
  consumer.addRecord(rec1);
  consumer.addRecord(rec2);
  const records = await consumer.poll(1);
  const iter = records[Symbol.iterator]();
  assert.equal(iter.next().value, rec1);
  assert.equal(iter.next().value, rec2);
  assert.ok(iter.next().done);
});

test('testRe2JPatternSubscription', async () => {
  const consumer = newMock();
  // The core's message: the FFI refuses the null pattern before the mock,
  // whose own is "Topic pattern cannot be null".
  await assert.rejects(consumer.subscribe({ pattern: /** @type {never} */ (null) }), {
    constructor: IllegalArgumentError,
    message: 'Topic pattern to subscribe to cannot be null',
  });
  await assert.rejects(consumer.subscribe(new SubscriptionPattern('')), {
    constructor: IllegalArgumentError,
    message: 'Topic pattern cannot be empty',
  });

  const pattern = new SubscriptionPattern('t.*');
  await assert.rejects(consumer.subscribe({ pattern, callback: /** @type {never} */ (null) }), {
    constructor: IllegalArgumentError,
    message: 'RebalanceListener cannot be null',
  });

  await consumer.subscribe(pattern);
  assert.equal(consumer.subscription().size, 0);
  // Check that the subscription to pattern was successfully applied in the mock consumer (using a different
  // subscription type should fail)
  await assert.rejects(consumer.subscribe(['topic1']), {
    constructor: IllegalStateError,
    message: 'Subscription to topics, partitions and pattern are mutually exclusive',
  });
});

test('testRebalanceListener', async () => {
  /** @type {TopicPartition[]} */
  let revoked = [];
  /** @type {TopicPartition[]} */
  let assigned = [];
  const consumerRebalanceListener = {
    /** @param {{ partitions: import('@confluentinc/kafka-javascript/common').TopicPartitionSet }} options */
    onPartitionsRevoked({ partitions }) {
      revoked = [...partitions];
    },
    /** @param {{ partitions: import('@confluentinc/kafka-javascript/common').TopicPartitionSet }} options */
    onPartitionsAssigned({ partitions }) {
      if (partitions.size() === 0) {
        return;
      }
      assigned = [...partitions];
    },
  };
  const consumer = newMock();

  await consumer.subscribe({ topics: new Set(['test']), callback: consumerRebalanceListener });
  assert.equal((await consumer.poll(0)).count(), 0);
  const topicPartitionList = [TP0, TP1];
  await consumer.rebalance(topicPartitionList);

  assert.equal(revoked.length, 0);
  assert.equal(assigned.length, 2);
  assert.ok(assigned.some((tp) => tp.equals(topicPartitionList[0])));
  assert.ok(assigned.some((tp) => tp.equals(topicPartitionList[1])));

  await consumer.rebalance([]);
  assert.equal(assigned.length, 2);
  assert.ok(revoked.some((tp) => tp.equals(topicPartitionList[0])));
  assert.ok(revoked.some((tp) => tp.equals(topicPartitionList[1])));

  await consumer.rebalance([topicPartitionList[0]]);
  assert.equal(assigned.length, 1);
  assert.ok(assigned.some((tp) => tp.equals(topicPartitionList[0])));

  await consumer.rebalance([topicPartitionList[1]]);
  assert.equal(assigned.length, 1);
  assert.ok(assigned.some((tp) => tp.equals(topicPartitionList[1])));
  assert.equal(revoked.length, 1);
  assert.ok(revoked.some((tp) => tp.equals(topicPartitionList[0])));
});

test('shouldReturnMaxPollRecords', async () => {
  const consumer = newMock();
  await consumer.assign([TP0]);
  consumer.updateBeginningOffsets(offsets([[TP0, 0n]]));

  for (let offset = 0; offset < 10; offset++) consumer.addRecord(record(0, offset));

  consumer.setMaxPollRecords(2n);

  let records = await consumer.poll(1);
  assert.equal(records.count(), 2);

  records = await consumer.poll(1);
  assert.equal(records.count(), 2);

  consumer.setMaxPollRecords(2n ** 63n - 1n);

  records = await consumer.poll(1);
  assert.equal(records.count(), 6);

  records = await consumer.poll(1);
  assert.ok(records.isEmpty());
});

test('testLosePartitionsCallsOnPartitionsLost', async () => {
  const tp0 = TP0;
  const tp1 = TP1;
  const assigned = [tp0, tp1];

  /** @type {TopicPartition[]} */
  const lost = [];
  /** @type {TopicPartition[]} */
  const revoked = [];
  const consumer = newMock();
  await consumer.subscribe({
    topics: ['test'],
    callback: {
      onPartitionsRevoked({ partitions }) {
        revoked.push(...partitions);
      },
      onPartitionsAssigned() {},
      onPartitionsLost({ partitions }) {
        lost.push(...partitions);
      },
    },
  });

  await consumer.rebalance(assigned);
  await consumer.losePartitions([tp0]);

  assert.deepEqual(lost.map(String), [String(tp0)]);
  assert.equal(revoked.length, 0);
});

test('testLosePartitionsRemovesFromAssignment', async () => {
  const consumer = newMock();
  await consumer.subscribe(['test']);
  await consumer.rebalance([TP0, TP1]);
  await consumer.losePartitions([TP0]);

  assert.ok(!consumer.assignment().has(TP0));
  assert.ok(consumer.assignment().has(TP1));
});

test('testLosePartitionsThrowsIfNotAssigned', async () => {
  const consumer = newMock();
  await consumer.subscribe(['test']);
  await consumer.rebalance([TP0]);

  await assert.rejects(consumer.losePartitions([TP1]), {
    constructor: IllegalStateError,
    message: 'Cannot lose partitions that are not currently assigned: [test-1]',
  });
});

test('testLosePartitionsClearsOnlyLostRecords', async () => {
  const consumer = newMock();
  await consumer.subscribe(['test']);
  await consumer.rebalance([TP0, TP1]);
  consumer.updateBeginningOffsets(
    offsets([
      [TP0, 0n],
      [TP1, 0n],
    ]),
  );
  await consumer.seek({ partition: TP0, offset: 0 });
  await consumer.seek({ partition: TP1, offset: 0 });

  consumer.addRecord(record(0, 0));
  consumer.addRecord(record(1, 0));

  await consumer.losePartitions([TP0]);

  const records = await consumer.poll(1);
  assert.equal(records.count(), 1);

  const [rec] = records;
  assert.ok(TP1.equals(new TopicPartition({ topic: rec.topic, partition: rec.partition })));
});

// ---- the constructor --------------------------------------------------------

test('testLosePartitionsThenRebalance', async () => {
  const tp0 = TP0;
  const tp1 = TP1;
  const tp2 = new TopicPartition({ topic: 'test', partition: 2 });

  /** @type {TopicPartition[]} */
  let assigned = [];
  const consumer = newMock();
  await consumer.subscribe({
    topics: ['test'],
    callback: {
      onPartitionsRevoked() {},
      onPartitionsAssigned({ partitions }) {
        assigned.push(...partitions);
      },
    },
  });

  await consumer.rebalance([tp0, tp1]);
  assigned = [];

  await consumer.losePartitions([tp0]);
  await consumer.rebalance([tp1, tp2]);

  assert.deepEqual(assigned.map(String), [String(tp2)]);
  assert.deepEqual([...consumer.assignment()].map(String).sort(), [String(tp1), String(tp2)]);
});

test("the constructor takes Java's strategies, with the core's messages for the others", async () => {
  for (const strategy of ['earliest', 'latest', 'none', 'by_duration:PT1H']) {
    await newMock(strategy).close();
    await new MockConsumer({ offsetResetStrategy: strategy }).close();
  }
  assert.throws(() => new MockConsumer('bad'), {
    constructor: IllegalArgumentError,
    message: 'Unknown auto offset reset strategy: bad',
  });
  // Java matches the names as they are written.
  assert.throws(() => new MockConsumer('EARLIEST'), {
    constructor: IllegalArgumentError,
    message: 'Unknown auto offset reset strategy: EARLIEST',
  });
  assert.throws(() => new MockConsumer('by_duration'), {
    constructor: IllegalArgumentError,
    message: '<:duration> part is missing in by_duration auto offset reset strategy.',
  });
  assert.throws(() => new MockConsumer('by_duration:x'), {
    constructor: IllegalArgumentError,
    message: 'Unable to parse duration string in by_duration offset reset strategy.',
  });
  assert.throws(() => new MockConsumer(/** @type {never} */ (null)), {
    constructor: IllegalArgumentError,
    message: 'Auto offset reset strategy is null',
  });
  assert.throws(() => new MockConsumer({ offsetResetStrategy: /** @type {never} */ (null) }), {
    constructor: IllegalArgumentError,
    message: 'Auto offset reset strategy is null',
  });
});

test("the constructor's forms", () => {
  const forms = 'MockConsumer() takes one of (offsetResetStrategy)';
  // @ts-expect-error -- Java has no no-argument constructor
  assert.throws(() => new MockConsumer(), { constructor: IllegalArgumentError, message: `${forms}; got ()` });
  // @ts-expect-error -- not a string
  assert.throws(() => new MockConsumer(1), { constructor: IllegalArgumentError, message: `${forms}; got a number` });
  // @ts-expect-error -- one argument only
  assert.throws(() => new MockConsumer('earliest', 1), {
    constructor: IllegalArgumentError,
    message: `${forms}; got 2 arguments, not one object of named parameters`,
  });
  // @ts-expect-error -- an unknown key
  assert.throws(() => new MockConsumer({ offsetResetStrategy: 'earliest', other: 1 }), {
    constructor: IllegalArgumentError,
    message: `${forms}; got (offsetResetStrategy, other)`,
  });
  // @ts-expect-error -- undefined is not a value
  assert.throws(() => new MockConsumer(undefined), {
    constructor: TypeError,
    message: "offsetResetStrategy must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  // @ts-expect-error -- not a string
  assert.throws(() => new MockConsumer({ offsetResetStrategy: 1 }), {
    constructor: TypeError,
    message: 'offsetResetStrategy must be a string',
  });
});

// ---- the records ------------------------------------------------------------

test('poll() returns the very records added, which keep their own headers; the C record carries the leader epoch', async () => {
  const consumer = newMock();
  await consumer.assign([TP0]);
  consumer.updateBeginningOffsets(offsets([[TP0, 0n]]));
  const headers = new Headers([new RecordHeader({ key: 'h', value: Buffer.from('v') })]);
  const added = new ConsumerRecord({
    topic: 'test',
    partition: 0,
    offset: 0,
    timestamp: 5,
    timestampType: TimestampType.LOG_APPEND_TIME,
    serializedKeySize: 3,
    serializedValueSize: 4,
    key: 'key',
    value: 'value',
    headers,
    leaderEpoch: 7,
  });
  consumer.addRecord({ record: added });
  const records = await consumer.poll({ timeout: 0 });
  const [polled] = records;
  assert.equal(polled, added);
  assert.equal(polled.headers, headers);
  assert.equal(polled.key, 'key');
  // The mock reads the leader epoch from the C record, as Java's reads the record's.
  assert.ok(new OffsetAndMetadata({ offset: 1, leaderEpoch: 7, metadata: '' }).equals(records.nextOffsets().get(TP0)));
  // An empty poll returns an empty ConsumerRecords.
  assert.ok((await consumer.poll(0)).isEmpty());
});

test('addRecord refuses a record of a partition not assigned, and a null one', async () => {
  const consumer = newMock();
  assert.throws(() => consumer.addRecord(record(0, 0)), {
    constructor: IllegalStateError,
    message: 'Cannot add records for a partition that is not assigned to the consumer',
  });
  // Java dereferences the record: a NullPointerException.
  // @ts-expect-error -- not null in the types
  assert.throws(() => consumer.addRecord(null), { constructor: TypeError, message: 'record must not be null' });
  await consumer.assign([TP0]);
  consumer.updateBeginningOffsets(offsets([[TP0, 0n]]));
  // The refused records were not kept: the poll returns only the one added after.
  const kept = record(0, 3);
  consumer.addRecord(kept);
  assert.deepEqual([...(await consumer.poll(0))], [kept]);
});

test("addRecord's, setMaxPollRecords' and the offset updates' forms", () => {
  const consumer = newMock();
  /** @type {Array<[string, string, (arg: unknown) => unknown]>} */
  const single = [
    ['addRecord', 'record', (a) => consumer.addRecord(/** @type {never} */ (a))],
    ['setMaxPollRecords', 'maxPollRecords', (a) => consumer.setMaxPollRecords(/** @type {never} */ (a))],
    ['setPollException', 'exception', (a) => consumer.setPollException(/** @type {never} */ (a))],
    ['setOffsetsException', 'exception', (a) => consumer.setOffsetsException(/** @type {never} */ (a))],
    ['updateBeginningOffsets', 'newOffsets', (a) => consumer.updateBeginningOffsets(/** @type {never} */ (a))],
    ['updateEndOffsets', 'newOffsets', (a) => consumer.updateEndOffsets(/** @type {never} */ (a))],
    ['updateDurationOffsets', 'newOffsets', (a) => consumer.updateDurationOffsets(/** @type {never} */ (a))],
  ];
  for (const [method, name, call] of single) {
    assert.throws(() => call('x'), { constructor: IllegalArgumentError, message: `${method}() takes one of (${name}); got a string` });
    assert.throws(() => call({ other: 1 }), {
      constructor: IllegalArgumentError,
      message: `${method}() takes one of (${name}); got (other)`,
    });
    assert.throws(() => call({ [name]: undefined }), {
      constructor: TypeError,
      message: `${name} must not be undefined. Leave the key out, or pass null for Java's null`,
    });
    const fn = /** @type {Function} */ (/** @type {Record<string, unknown>} */ (/** @type {unknown} */ (consumer))[method]);
    assert.throws(() => fn.call(consumer), {
      constructor: IllegalArgumentError,
      message: `${method}() takes one of (${name}); got ()`,
    });
    assert.throws(() => fn.call(consumer, null, 1), {
      constructor: IllegalArgumentError,
      message: `${method}() takes one of (${name}); got 2 arguments, not one object of named parameters`,
    });
  }
  // The run-time type checks.
  assert.throws(() => consumer.addRecord({ record: /** @type {never} */ ({}) }), {
    constructor: TypeError,
    message: 'record must be a ConsumerRecord',
  });
  assert.throws(() => consumer.setMaxPollRecords(1.5), {
    constructor: TypeError,
    message: 'maxPollRecords must be a bigint or an integer',
  });
  assert.throws(() => consumer.setMaxPollRecords(2 ** 53), { constructor: RangeError });
  assert.throws(() => consumer.setPollException(/** @type {never} */ (new Error('x'))), {
    constructor: TypeError,
    message: 'exception must be a KafkaError or null',
  });
  assert.throws(() => consumer.updateEndOffsets(/** @type {never} */ (new Map([[TP0, 1n]]))), {
    constructor: TypeError,
    message:
      'newOffsets must be a TopicPartitionMap, not a Map: a Map keyed by TopicPartition compares its keys by identity, so build one with new TopicPartitionMap(entries)',
  });
});

test("setMaxPollRecords refuses a value below 1, with Java's message from the core", () => {
  const consumer = newMock();
  for (const value of [0, -1, 0n]) {
    assert.throws(() => consumer.setMaxPollRecords(value), {
      constructor: IllegalArgumentError,
      message: 'MaxPollRecords must be strictly superior to 0',
    });
  }
  consumer.setMaxPollRecords({ maxPollRecords: 1 });
});

test("an offset update refuses a null offset, which the FFI would read as 0; a null map or key is the FFI's TypeError", () => {
  const consumer = newMock();
  for (const method of /** @type {const} */ (['updateBeginningOffsets', 'updateEndOffsets', 'updateDurationOffsets'])) {
    assert.throws(() => consumer[method](/** @type {never} */ (offsets([[TP0, /** @type {never} */ (null)]]))), {
      constructor: TypeError,
      message: 'newOffsets must not contain a null offset',
    });
    // @ts-expect-error -- not null in the types
    assert.throws(() => consumer[method](null), { constructor: TypeError, message: 'new_offsets must not be null' });
    assert.throws(() => consumer[method](/** @type {never} */ (offsets([[/** @type {never} */ (null), 1n]]))), { constructor: TypeError });
  }
});

// ---- the injected errors ----------------------------------------------------

test('an error given to setPollException comes back from poll() as itself, once; null clears it', async () => {
  const consumer = newMock();
  const error = new KafkaError('poll failed');
  consumer.setPollException(error);
  await assert.rejects(consumer.poll(0), (e) => e === error);
  assert.ok((await consumer.poll(0)).isEmpty());
  consumer.setPollException({ exception: new KafkaError('cleared') });
  consumer.setPollException(null);
  assert.ok((await consumer.poll(0)).isEmpty());
});

test('an error given to setOffsetsException comes back from beginningOffsets() and endOffsets() as itself, once', async () => {
  const consumer = newMock();
  consumer.updateBeginningOffsets(offsets([[TP0, 3n]]));
  consumer.updateEndOffsets(offsets([[TP0, 9n]]));
  const first = new KafkaError('offsets failed');
  consumer.setOffsetsException(first);
  await assert.rejects(consumer.beginningOffsets([TP0]), (e) => e === first);
  assert.equal((await consumer.beginningOffsets([TP0])).get(TP0), 3n);
  const second = new KafkaError({ message: 'again' });
  consumer.setOffsetsException({ exception: second });
  await assert.rejects(consumer.endOffsets({ partitions: [TP0], timeout: 5 }), (e) => e === second);
  assert.equal((await consumer.endOffsets([TP0])).get(TP0), 9n);
  consumer.setOffsetsException(new KafkaError('cleared'));
  consumer.setOffsetsException(null);
  assert.equal((await consumer.endOffsets([TP0])).get(TP0), 9n);
});

test("beginningOffsets() and endOffsets() of a partition with none, with Java's messages from the core", async () => {
  const consumer = newMock();
  await assert.rejects(consumer.beginningOffsets([TP0]), {
    constructor: IllegalStateError,
    message: 'The partition test-0 does not have a beginning offset.',
  });
  await assert.rejects(consumer.endOffsets([TP0]), {
    constructor: IllegalStateError,
    message: 'The partition test-0 does not have an end offset.',
  });
});

// ---- rebalance and losePartitions -------------------------------------------

test("rebalance's and losePartitions' forms; a null collection is the FFI's TypeError", async () => {
  const consumer = newMock();
  await consumer.subscribe(['test']);
  await consumer.rebalance({ newAssignment: [TP0, TP1] });
  assert.deepEqual([...consumer.assignment()].map(String).sort(), ['test-0', 'test-1']);
  await consumer.losePartitions({ partitionsLost: new Set([TP1]) });
  assert.deepEqual([...consumer.assignment()].map(String), ['test-0']);
  for (const [method, name] of /** @type {const} */ ([
    ['rebalance', 'newAssignment'],
    ['losePartitions', 'partitionsLost'],
  ])) {
    const call = /** @type {(...args: unknown[]) => Promise<void>} */ (consumer[method].bind(consumer));
    await assert.rejects(call('x'), { constructor: IllegalArgumentError, message: `${method}() takes one of (${name}); got a string` });
    await assert.rejects(call(), { constructor: IllegalArgumentError, message: `${method}() takes one of (${name}); got ()` });
    await assert.rejects(call({ [name]: undefined }), {
      constructor: TypeError,
      message: `${name} must not be undefined. Leave the key out, or pass null for Java's null`,
    });
    await assert.rejects(call([TP0], 1), {
      constructor: IllegalArgumentError,
      message: `${method}() takes one of (${name}); got 2 arguments, not one object of named parameters`,
    });
  }
  await assert.rejects(consumer.rebalance(/** @type {never} */ (null)), {
    constructor: TypeError,
    message: 'new_assignment must not be null',
  });
  await assert.rejects(consumer.losePartitions(/** @type {never} */ (null)), {
    constructor: TypeError,
    message: 'partitions_lost must not be null',
  });
});

test("rebalance needs a subscription, with the core's message", async () => {
  const consumer = newMock();
  await consumer.assign([TP0]);
  await assert.rejects(consumer.rebalance([TP0]), {
    constructor: IllegalArgumentError,
    message: 'Attempt to dynamically assign partitions while manual assignment in use',
  });
});

// ---- the Consumer methods on the mock ---------------------------------------

test("offsetsForTimes is not supported, with Java's message from the core", async () => {
  const consumer = newMock();
  const query = new TopicPartitionMap([[TP0, 1n]]);
  await assert.rejects(consumer.offsetsForTimes(query), { constructor: UnsupportedOperationError, message: 'Not implemented yet.' });
  await assert.rejects(consumer.offsetsForTimes({ timestampsToSearch: query, timeout: 1 }), {
    constructor: UnsupportedOperationError,
    message: 'Not implemented yet.',
  });
});

test("groupMetadata() is Java's dummy, without a group.id", () => {
  const consumer = newMock();
  const metadata = consumer.groupMetadata();
  assert.ok(metadata instanceof ConsumerGroupMetadata);
  assert.equal(metadata.groupId(), 'dummy.group.id');
  assert.equal(metadata.generationId(), 1);
  assert.equal(metadata.memberId(), '1');
  assert.equal(metadata.groupInstanceId(), null);
});

test('currentLag() is the end offset less the position, or 0 without an end offset', async () => {
  const consumer = newMock();
  await consumer.assign([TP0]);
  consumer.updateBeginningOffsets(offsets([[TP0, 2n]]));
  assert.equal(await consumer.currentLag(TP0), 0n);
  consumer.updateEndOffsets(offsets([[TP0, 10n]]));
  assert.equal(await consumer.currentLag({ topicPartition: TP0 }), 8n);
});

test("the position of a partition with no reset offset, with Java's messages from the core", async () => {
  const none = newMock('none');
  await none.assign([TP0]);
  await assert.rejects(none.poll(0), {
    constructor: NoOffsetForPartitionError,
    message: 'Undefined offset with no reset policy for partition: test-0',
  });
  const latest = newMock('latest');
  await latest.assign([TP0]);
  await assert.rejects(latest.position(TP0), {
    constructor: IllegalStateError,
    message: "MockConsumer didn't have end offset specified, but tried to seek to end",
  });
  await assert.rejects(latest.position(TP1), {
    constructor: IllegalArgumentError,
    message: 'You can only check the position for partitions assigned to this consumer.',
  });
});

test('updatePartitions is read back by partitionsFor and listTopics', async () => {
  const consumer = newMock();
  const leader = new Node({ id: 1, host: 'h1', port: 9092, rack: 'r1', isFenced: false });
  const other = new Node({ id: 2, host: 'h2', port: 9093 });
  const infos = [
    new PartitionInfo({
      topic: 'test',
      partition: 0,
      leader,
      replicas: [leader, other],
      inSyncReplicas: [leader],
      offlineReplicas: [other],
    }),
    new PartitionInfo({ topic: 'test', partition: 1, leader: null, replicas: [], inSyncReplicas: [] }),
  ];
  assert.deepEqual(await consumer.partitionsFor('test'), []);
  consumer.updatePartitions({ topic: 'test', partitions: infos });
  const found = await consumer.partitionsFor({ topic: 'test', timeout: 1 });
  assert.equal(found.length, 2);
  for (let i = 0; i < 2; i++) assert.ok(infos[i].equals(found[i]), String(found[i]));
  const topics = await consumer.listTopics();
  assert.deepEqual([...topics.keys()], ['test']);
  assert.ok(infos[0].equals(/** @type {PartitionInfo[]} */ (topics.get('test'))[0]));
  // @ts-expect-error -- Java's two parameters are given by name only
  assert.throws(() => consumer.updatePartitions('test'), {
    constructor: IllegalArgumentError,
    message: 'updatePartitions() takes one of (topic, partitions); got a string',
  });
  assert.throws(() => consumer.updatePartitions({ topic: 'test', partitions: /** @type {never} */ ('x') }), {
    constructor: TypeError,
    message: 'partitions must be an iterable of PartitionInfo',
  });
});

test("updatePartitions' nulls get the FFI's TypeErrors, where Java's mock stores them", () => {
  const consumer = newMock();
  /** @type {Array<[unknown, unknown, string]>} */
  const cases = [
    [null, [], 'topic must not be null'],
    ['t', null, 'partitions must not be null'],
    ['t', [null], 'partitions must not contain null'],
    ['t', [new PartitionInfo({ topic: null, partition: 0, leader: null, replicas: [], inSyncReplicas: [] })], 'topic must not be null'],
    ['t', [new PartitionInfo({ topic: 't', partition: 0, leader: null, replicas: null, inSyncReplicas: [] })], 'replicas must not be null'],
    [
      't',
      [new PartitionInfo({ topic: 't', partition: 0, leader: null, replicas: /** @type {never} */ ([null]), inSyncReplicas: [] })],
      'replicas must not contain null',
    ],
    [
      't',
      [new PartitionInfo({ topic: 't', partition: 0, leader: new Node({ id: 1, host: null, port: 1 }), replicas: [], inSyncReplicas: [] })],
      'host must not be null',
    ],
  ];
  for (const [topic, partitions, message] of cases) {
    assert.throws(() => consumer.updatePartitions({ topic: /** @type {never} */ (topic), partitions: /** @type {never} */ (partitions) }), {
      constructor: TypeError,
      message,
    });
  }
});

test('lastPollTimeout() is null until a poll, then its timeout in milliseconds', async () => {
  const consumer = newMock();
  assert.equal(consumer.lastPollTimeout(), null);
  await consumer.poll(25);
  assert.equal(consumer.lastPollTimeout(), 25);
  await consumer.poll({ timeout: Infinity });
  assert.equal(consumer.lastPollTimeout(), Infinity);
  // @ts-expect-error -- it takes no argument
  assert.throws(() => consumer.lastPollTimeout(1), {
    constructor: TypeError,
    message: 'lastPollTimeout() only reads: it takes no argument',
  });
});

test('scheduleNopPollTask() queues a task that does nothing for the next poll', async () => {
  const consumer = newMock();
  await consumer.assign([TP0]);
  consumer.updateBeginningOffsets(offsets([[TP0, 0n]]));
  const added = record(0, 0);
  consumer.addRecord(added);
  consumer.scheduleNopPollTask();
  assert.deepEqual([...(await consumer.poll(0))], [added]);
  // @ts-expect-error -- it takes no argument
  assert.throws(() => consumer.scheduleNopPollTask(1), {
    constructor: TypeError,
    message: 'scheduleNopPollTask() takes no argument: leave it out rather than pass undefined',
  });
});

// ---- a closed mock ----------------------------------------------------------

test("a closed mock: closed() answers true, and its calls get the core's message", async () => {
  const consumer = newMock();
  assert.equal(consumer.closed(), false);
  await consumer.close({ option: CloseOptions.timeout({ timeout: 5 }) });
  assert.equal(consumer.closed(), true);
  // @ts-expect-error -- it takes no argument
  assert.throws(() => consumer.closed(1), { constructor: TypeError, message: 'closed() only reads: it takes no argument' });
  await assert.rejects(consumer.poll(0), { constructor: IllegalStateError, message: CLOSED });
  await assert.rejects(consumer.subscribe(['test']), { constructor: IllegalStateError, message: CLOSED });
  await assert.rejects(consumer.assign([TP0]), { constructor: IllegalStateError, message: CLOSED });
  await assert.rejects(consumer.commitSync(), { constructor: IllegalStateError, message: CLOSED });
  await assert.rejects(consumer.position(TP0), { constructor: IllegalStateError, message: CLOSED });
  await assert.rejects(consumer.partitionsFor('test'), { constructor: IllegalStateError, message: CLOSED });
  assert.throws(() => consumer.addRecord(record(0, 0)), { constructor: IllegalStateError, message: CLOSED });
  assert.throws(() => consumer.updatePartitions({ topic: 'test', partitions: [] }), { constructor: IllegalStateError, message: CLOSED });
  // The FFI refuses a null record before the core's closed check.
  // @ts-expect-error -- not null in the types
  assert.throws(() => consumer.addRecord(null), { constructor: TypeError, message: 'record must not be null' });
  // metrics() answers through the consumer's handle, which outlives the
  // close: an empty map, where Java's mock throws CLOSED.
  assert.equal(consumer.metrics().size(), 0);
  // A second close is harmless.
  await consumer.close();
});

test("close(null) is the FFI's TypeError and closes nothing", async () => {
  const consumer = newMock();
  await assert.rejects(consumer.close({ option: /** @type {never} */ (null) }), {
    constructor: TypeError,
    message: 'options must not be null',
  });
  assert.equal(consumer.closed(), false);
  await consumer.poll(0);
});

// ---- one call at a time -----------------------------------------------------

test('a mock takes one call at a time; the setters with no error to report are ignored while a call is pending', async () => {
  const consumer = newMock();
  await consumer.assign([TP0]);
  consumer.updateBeginningOffsets(offsets([[TP0, 0n]]));
  consumer.updateEndOffsets(offsets([[TP0, 0n]]));
  const probe = record(0, 0);
  // The FFI's mock answers a poll at once, so the window in which it is
  // pending is short: each round updates the end offset to its own value,
  // then tries a call the FFI refuses while the poll is pending. The flag is
  // taken inside poll() and released once, so an update made before a
  // refused call was made while the poll was pending.
  let busy = null;
  for (let round = 1; busy === null && round <= 10000; round++) {
    const poll = consumer.poll(round);
    consumer.updateEndOffsets(offsets([[TP0, BigInt(round)]]));
    const closed = consumer.closed();
    const timeout = consumer.lastPollTimeout();
    try {
      consumer.addRecord(probe);
    } catch (e) {
      if (!(e instanceof ConcurrentModificationError)) throw e;
      assert.equal(e.message, CONCURRENT);
      busy = { round, closed, timeout };
    }
    await poll;
  }
  assert.ok(busy !== null, 'no round found the poll pending');
  // closed() and lastPollTimeout() answer while the poll is pending.
  assert.equal(busy.closed, false);
  assert.ok(busy.timeout === null || busy.timeout === busy.round - 1 || busy.timeout === busy.round);
  // The update made while the poll was pending was ignored (logged by the
  // core), where Java's synchronized mock waits and applies it: the end offset
  // is an earlier round's (an earlier update may have been ignored too).
  const end = /** @type {bigint} */ ((await consumer.endOffsets([TP0])).get(TP0));
  assert.ok(end < BigInt(busy.round), `${end} is the ignored round's ${busy.round}`);
  // A waiting call made while another is pending is refused too: under a
  // manual assignment, a rebalance that runs fails with the core's own error
  // instead.
  let refused = false;
  for (let round = 1; !refused && round <= 10000; round++) {
    const poll = consumer.poll(0);
    const outcome = await consumer.rebalance([TP0]).then(
      () => 'ran',
      (/** @type {Error} */ e) => e,
    );
    await poll;
    if (outcome instanceof ConcurrentModificationError) {
      assert.equal(outcome.message, CONCURRENT);
      refused = true;
    } else {
      assert.ok(outcome instanceof IllegalArgumentError, String(outcome));
    }
  }
  assert.ok(refused, 'no round found the poll pending');
});

/**
 * Starts a poll of `consumer`, runs `during(round)` and tries `assignment()`,
 * round after round, until the poll was still pending when `assignment()` was
 * called: the FFI's mock answers a poll at once, and the single-owner flag,
 * taken inside poll() and released once, was then held for `during` too.
 *
 * @param {MockConsumer<string, string>} consumer
 * @param {(round: number) => void} during
 * @returns {Promise<number>} the round
 */
async function pendingRound(consumer, during) {
  for (let round = 1; round <= 10000; round++) {
    const poll = consumer.poll(0).catch(() => {});
    during(round);
    let pending = false;
    try {
      consumer.assignment();
    } catch (e) {
      if (!(e instanceof ConcurrentModificationError)) throw e;
      pending = true;
    }
    await poll;
    if (pending) return round;
  }
  return assert.fail('no round found the poll pending');
}

test('each ignored setter whose effect can be seen leaves it unseen when it was called while a poll was pending', async () => {
  const consumer = newMock();
  consumer.updateBeginningOffsets(offsets([[TP0, 0n]]));
  let round = await pendingRound(consumer, (r) => consumer.updateBeginningOffsets(offsets([[TP0, BigInt(r)]])));
  assert.ok(/** @type {bigint} */ ((await consumer.beginningOffsets([TP0])).get(TP0)) < BigInt(round));

  const duration = newMock('by_duration:PT1H');
  duration.updateDurationOffsets(offsets([[TP0, 0n]]));
  round = await pendingRound(duration, (r) => duration.updateDurationOffsets(offsets([[TP0, BigInt(r)]])));
  // The reset reads the duration offset.
  await duration.assign([TP0]);
  assert.ok((await duration.position(TP0)) < BigInt(round));

  // The error set while the poll was pending is never thrown.
  /** @type {KafkaError[]} */
  const pollErrors = [];
  round = await pendingRound(consumer, (r) => consumer.setPollException((pollErrors[r] = new KafkaError(`poll ${r}`))));
  const next = await consumer.poll(0).then(
    () => null,
    (/** @type {unknown} */ e) => e,
  );
  assert.notEqual(next, pollErrors[round]);

  consumer.updateEndOffsets(offsets([[TP0, 4n]]));
  /** @type {KafkaError[]} */
  const offsetsErrors = [];
  round = await pendingRound(consumer, (r) => consumer.setOffsetsException((offsetsErrors[r] = new KafkaError(`offsets ${r}`))));
  for (let i = 0; i < 2; i++) {
    const answer = await consumer.endOffsets([TP0]).then(
      (map) => map.get(TP0),
      (/** @type {unknown} */ e) => e,
    );
    assert.notEqual(answer, offsetsErrors[round]);
  }
});

test('assignment(), subscription(), paused() and groupMetadata() throw ConcurrentModificationError while a call is pending', async () => {
  const consumer = newMock();
  for (const getter of /** @type {const} */ (['assignment', 'subscription', 'paused', 'groupMetadata'])) {
    let refused = false;
    for (let round = 1; !refused && round <= 10000; round++) {
      const poll = consumer.poll(0);
      try {
        consumer[getter]();
      } catch (e) {
        if (!(e instanceof ConcurrentModificationError)) throw e;
        assert.equal(e.message, CONCURRENT);
        refused = true;
      }
      await poll;
    }
    assert.ok(refused, `${getter}() was never refused`);
  }
});

test("the Consumer methods' null arguments get the FFI's errors, where Java's mock answers or throws NullPointerException", async () => {
  const consumer = newMock();
  // Java's mock answers [] and OptionalLong.of(0).
  await assert.rejects(consumer.partitionsFor(/** @type {never} */ (null)), { constructor: TypeError, message: 'topic must not be null' });
  await assert.rejects(consumer.partitionsFor({ topic: /** @type {never} */ (null), timeout: 1 }), {
    constructor: TypeError,
    message: 'topic must not be null',
  });
  await assert.rejects(consumer.currentLag(/** @type {never} */ (null)), {
    constructor: TypeError,
    message: 'topic_partition must not be null',
  });
  // Java's mock throws NullPointerException; these are Java's KafkaConsumer messages.
  await assert.rejects(consumer.subscribe({ topics: /** @type {never} */ (null) }), {
    constructor: IllegalArgumentError,
    message: 'Topic collection to subscribe to cannot be null',
  });
  await assert.rejects(consumer.assign({ partitions: /** @type {never} */ (null) }), {
    constructor: IllegalArgumentError,
    message: 'Topic partitions collection to assign to cannot be null',
  });
  for (const method of /** @type {const} */ (['seekToBeginning', 'seekToEnd'])) {
    await assert.rejects(consumer[method](/** @type {never} */ (null)), {
      constructor: IllegalArgumentError,
      message: 'Partitions collection cannot be null',
    });
  }
});

test("a null partition or topic given to assign() or subscribe() goes to the FFI, which refuses it, where Java's mock stores it", async () => {
  const consumer = newMock();
  // Java's mock throws NullPointerException at partition.topic().
  await assert.rejects(consumer.assign([/** @type {never} */ (null)]), {
    constructor: TypeError,
    message: 'partitions must not contain null',
  });
  // Java's mock assigns null-3 and subscribes to null.
  await assert.rejects(consumer.assign([new TopicPartition({ topic: /** @type {never} */ (null), partition: 3 })]), {
    constructor: TypeError,
    message: 'topic must not be null',
  });
  await assert.rejects(consumer.subscribe(['a', /** @type {never} */ (null)]), {
    constructor: TypeError,
    message: 'topics must not contain null',
  });
  // Nothing the test did not give was assigned or subscribed to.
  assert.equal(consumer.assignment().size(), 0);
  assert.equal(consumer.subscription().size, 0);
});

test("a negative timeout is the FFI's IllegalArgumentError, where Java's mock ignores it", async () => {
  const consumer = newMock();
  await consumer.assign([TP0]);
  consumer.updateBeginningOffsets(offsets([[TP0, 0n]]));
  const negative = { constructor: IllegalArgumentError, message: 'Timeout must not be negative' };
  await assert.rejects(consumer.poll(-1), negative);
  // Java's mock records the timeout of every poll.
  assert.equal(consumer.lastPollTimeout(), null);
  await assert.rejects(consumer.position({ partition: TP0, timeout: -1 }), negative);
  await assert.rejects(consumer.commitSync({ timeout: -1 }), negative);
  await assert.rejects(consumer.committed({ partitions: [TP0], timeout: -1 }), negative);
  await assert.rejects(consumer.beginningOffsets({ partitions: [TP0], timeout: -1 }), negative);
  await assert.rejects(consumer.listTopics(-1), negative);
});

// ---- liveness ---------------------------------------------------------------

/**
 * Runs `script` in a child Node with the package root as its directory.
 *
 * @param {string} script
 * @param {number} timeout
 * @returns {import('node:child_process').SpawnSyncReturns<string>} the outcome
 */
function child(script, timeout) {
  return spawnSync(process.execPath, ['-e', script], { cwd: path.join(__dirname, '..', '..', '..'), encoding: 'utf8', timeout });
}

const CHILD_MOCK = `
  const { MockConsumer } = require('@confluentinc/kafka-javascript/consumer');
  const c = new MockConsumer('earliest');
`;

test('a mock keeps Node running until close(), emits no preview warning, and every call made on it gets its answer', () => {
  // Never closed: Node keeps running until the child is killed.
  const open = child(`${CHILD_MOCK} console.log('open');`, 3000);
  assert.equal(open.signal, 'SIGTERM');
  assert.equal(open.stdout, 'open\n');
  // A close the FFI refuses (null options) closes nothing.
  const refused = child(
    `${CHILD_MOCK} c.close({ option: null }).catch((e) => console.log('refused: ' + e.message + '; ' + c.closed()));`,
    3000,
  );
  assert.equal(refused.signal, 'SIGTERM', refused.stderr);
  assert.equal(refused.stdout, 'refused: options must not be null; false\n');
  // Closed: Node exits, after the answers of the calls made on it.
  const closed = child(
    `${CHILD_MOCK}
    c.close().then(() => {
      console.log('closed ' + c.closed());
      return c.poll(0).catch((e) => console.log('after: ' + e.message));
    }).then(() => c.close()).then(() => console.log('closed again'));`,
    30000,
  );
  assert.equal(closed.signal, null, closed.stderr);
  assert.equal(closed.status, 0, closed.stderr);
  assert.equal(closed.stdout, `closed true\nafter: ${CLOSED}\nclosed again\n`);
  assert.ok(!closed.stderr.includes('ExperimentalWarning'), closed.stderr);
});

// ---- the listeners and the commit callbacks ---------------------------------

test("a listener's forms on the mock, and its Promise, which rebalance() and losePartitions() await", async () => {
  const consumer = newMock();
  /** @type {string[]} */
  const seen = [];
  /** @type {() => void} */
  let release = () => {};
  const gate = new Promise((resolve) => (release = () => resolve(undefined)));
  await consumer.subscribe({
    pattern: new SubscriptionPattern('te.*'),
    callback: {
      onPartitionsRevoked() {},
      async onPartitionsAssigned({ partitions }) {
        await gate;
        seen.push(`assigned ${[...partitions].map(String)}`);
      },
    },
  });
  const rebalanced = consumer.rebalance([TP0]);
  let settled = false;
  void rebalanced.then(() => (settled = true));
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.equal(settled, false);
  release();
  await rebalanced;
  assert.deepEqual(seen, ['assigned test-0']);
  // A listener without onPartitionsLost: Java's default calls onPartitionsRevoked.
  await consumer.unsubscribe();
  await consumer.subscribe({
    topics: ['test'],
    callback: {
      onPartitionsRevoked: ({ partitions }) =>
        new Promise((resolve) => setImmediate(() => resolve(seen.push(`revoked ${[...partitions].map(String)}`)))),
      onPartitionsAssigned() {},
    },
  });
  await consumer.rebalance([TP0, TP1]);
  await consumer.losePartitions([TP1]);
  assert.deepEqual(seen, ['assigned test-0', 'revoked test-1']);
  // A missing required method, a null listener and one that is no object.
  await assert.rejects(consumer.subscribe({ topics: ['test'], callback: /** @type {never} */ ({ onPartitionsAssigned() {} }) }), {
    constructor: TypeError,
    message: 'on_partitions_revoked must not be null',
  });
  await assert.rejects(consumer.subscribe({ topics: ['test'], callback: /** @type {never} */ (null) }), {
    constructor: IllegalArgumentError,
    message: 'RebalanceListener cannot be null',
  });
  await assert.rejects(consumer.subscribe({ topics: ['test'], callback: /** @type {never} */ (() => {}) }), {
    constructor: TypeError,
    message: 'callback must be a ConsumerRebalanceListener',
  });
});

test("a listener's error comes back from rebalance() and losePartitions() as itself, as Java's mock calls the listener directly", async () => {
  for (const value of [new KafkaError('the listener failed'), new TypeError('boom'), 'a string', null, undefined]) {
    const consumer = newMock();
    await consumer.subscribe({
      topics: ['test'],
      callback: {
        onPartitionsRevoked() {},
        onPartitionsAssigned({ partitions }) {
          if (partitions.size() > 0) throw value;
        },
        onPartitionsLost: () => Promise.reject(value),
      },
    });
    await assert.rejects(consumer.rebalance([TP0]), (error) => error === value);
    await assert.rejects(consumer.losePartitions([TP0]), (error) => error === value);
  }
});

test("inside a mock's listener, the handle's commits, currentLag and sync reads work, its other calls are the core's UnsupportedOperationError, and the rest meet the single-owner flag", async () => {
  const consumer = newMock();
  await consumer.subscribe({ topics: ['test'], callback: { onPartitionsRevoked() {}, onPartitionsAssigned() {} } });
  await consumer.rebalance([TP0]);
  consumer.updateBeginningOffsets(offsets([[TP0, 0n]]));
  consumer.updateEndOffsets(offsets([[TP0, 4n]]));
  consumer.addRecord(record(0, 0));
  await consumer.poll(0);
  /** @type {Record<string, unknown>} */
  const out = {};
  const unsupported = 'ConsumerHandle async operations are not supported on a MockConsumer handle; drive the MockConsumer directly.';
  await consumer.subscribe({
    topics: ['test'],
    callback: {
      onPartitionsRevoked() {},
      async onPartitionsAssigned({ partitions }) {
        if (partitions.size() === 0) return;
        /**
         * @param {string} name
         * @param {() => unknown} call
         */
        const attempt = async (name, call) => {
          try {
            const value = await call();
            out[name] = value === undefined ? 'ok' : value;
          } catch (error) {
            out[name] = error instanceof Error ? `${error.constructor.name}: ${error.message}` : error;
          }
        };
        const committed = new TopicPartitionMap([[TP0, new OffsetAndMetadata(1)]]);
        await attempt('commitSync', () => consumer.commitSync());
        await attempt('commitSync(offsets)', () => consumer.commitSync(committed));
        await attempt('commitAsync', () => consumer.commitAsync());
        await attempt('currentLag', () => consumer.currentLag(TP0));
        await attempt('position', () => consumer.position(TP0));
        await attempt('seek', () => consumer.seek({ partition: TP0, offset: 0 }));
        await attempt('committed', () => consumer.committed(new Set([TP0])));
        await attempt('pause', () => consumer.pause([TP0]));
        await attempt('poll', () => consumer.poll(0));
        await attempt('assign', () => consumer.assign([TP0]));
        await attempt('subscription', () => consumer.subscription().size);
        // The handle's sync forms answer.
        await attempt('assignment', () => [...consumer.assignment()].map(String).join());
        await attempt('groupMetadata', () => consumer.groupMetadata().groupId());
      },
    },
  });
  await consumer.rebalance([TP0, TP1]);
  assert.deepEqual(out, {
    commitSync: 'ok',
    'commitSync(offsets)': 'ok',
    commitAsync: 'ok',
    currentLag: 3n,
    position: `UnsupportedOperationError: ${unsupported}`,
    seek: `UnsupportedOperationError: ${unsupported}`,
    committed: `UnsupportedOperationError: ${unsupported}`,
    pause: `UnsupportedOperationError: ${unsupported}`,
    poll: `ConcurrentModificationError: ${CONCURRENT}`,
    assign: `ConcurrentModificationError: ${CONCURRENT}`,
    subscription: `ConcurrentModificationError: ${CONCURRENT}`,
    assignment: 'test-0,test-1',
    groupMetadata: 'dummy.group.id',
  });
});

test("inside a mock's listener, its own calls: the setters with no error slot are ignored, the others refused, and the reads answer", async () => {
  const consumer = newMock();
  /** @type {Record<string, unknown>} */
  const out = {};
  await consumer.subscribe({
    topics: ['test'],
    callback: {
      onPartitionsRevoked() {},
      async onPartitionsAssigned({ partitions }) {
        if (partitions.size() === 0) return;
        /**
         * @param {string} name
         * @param {() => unknown} call
         */
        const attempt = async (name, call) => {
          try {
            const value = await call();
            out[name] = value === undefined ? 'ok' : value;
          } catch (error) {
            out[name] = error instanceof Error ? `${error.constructor.name}: ${error.message}` : error;
          }
        };
        // The setters with no error slot: the core logs and ignores them.
        await attempt('updateBeginningOffsets', () => consumer.updateBeginningOffsets(offsets([[TP0, 0n]])));
        await attempt('updateEndOffsets', () => consumer.updateEndOffsets(offsets([[TP0, 5n]])));
        await attempt('updateDurationOffsets', () => consumer.updateDurationOffsets(offsets([[TP0, 0n]])));
        await attempt('setPollException', () => consumer.setPollException(new KafkaError('poll')));
        await attempt('setOffsetsException', () => consumer.setOffsetsException(new KafkaError('offsets')));
        await attempt('scheduleNopPollTask', () => consumer.scheduleNopPollTask());
        // The others meet the single-owner flag.
        await attempt('addRecord', () => consumer.addRecord(record(0, 0)));
        await attempt('setMaxPollRecords', () => consumer.setMaxPollRecords(1));
        await attempt('rebalance', () => consumer.rebalance([TP0]));
        await attempt('losePartitions', () => consumer.losePartitions([TP0]));
        await attempt('close', () => consumer.close());
        // The reads through the handle answer.
        await attempt('closed', () => consumer.closed());
        await attempt('lastPollTimeout', () => consumer.lastPollTimeout());
        await attempt('metrics', () => typeof consumer.metrics().size());
        await attempt('wakeup', () => consumer.wakeup());
      },
    },
  });
  await consumer.rebalance([TP0]);
  const concurrent = `ConcurrentModificationError: ${CONCURRENT}`;
  assert.deepEqual(out, {
    updateBeginningOffsets: 'ok',
    updateEndOffsets: 'ok',
    updateDurationOffsets: 'ok',
    setPollException: 'ok',
    setOffsetsException: 'ok',
    scheduleNopPollTask: 'ok',
    addRecord: concurrent,
    setMaxPollRecords: concurrent,
    rebalance: concurrent,
    losePartitions: concurrent,
    close: concurrent,
    closed: false,
    lastPollTimeout: null,
    metrics: 'number',
    wakeup: 'ok',
  });
  // The ignored setters left nothing behind.
  await assert.rejects(consumer.endOffsets([TP0]), { message: 'The partition test-0 does not have an end offset.' });
  await assert.rejects(consumer.beginningOffsets([TP0]), { message: 'The partition test-0 does not have a beginning offset.' });
  // The wakeup() made inside the listener wakes the next poll.
  await assert.rejects(consumer.poll(0), { name: 'WakeupError', message: 'Mock consumer was woken up' });
  await assert.rejects(consumer.poll(0), {
    constructor: IllegalStateError,
    message: "MockConsumer didn't have beginning offset specified, but tried to seek to beginning",
  });
  assert.equal(consumer.closed(), false);
});

test("a mock keeps its listener after close(): rebalance() still calls it, as Java's mock does", async () => {
  const consumer = newMock();
  /** @type {string[]} */
  const seen = [];
  await consumer.subscribe({
    topics: ['test'],
    callback: {
      onPartitionsRevoked() {},
      onPartitionsAssigned({ partitions }) {
        seen.push([...partitions].map(String).join());
      },
    },
  });
  await consumer.close();
  await consumer.rebalance([TP0]).catch(() => {});
  assert.deepEqual(seen, ['test-0']);
});

test("a mock's commit callback gets the map committed, an empty one included, and its error comes back from commitAsync() as itself", async () => {
  const consumer = newMock();
  await consumer.assign([TP0]);
  /** @type {Array<[string | null, string | null]>} */
  const seen = [];
  /** @param {{ offsets: TopicPartitionMap<OffsetAndMetadata> | null, exception: Error | null }} answer */
  const callback = ({ offsets: committed, exception }) => {
    seen.push([committed === null ? null : String(committed), exception === null ? null : exception.message]);
  };
  await consumer.commitAsync({ offsets: new TopicPartitionMap([[TP0, new OffsetAndMetadata(3)]]), callback });
  await consumer.commitAsync({ offsets: new TopicPartitionMap(), callback });
  // Java's mock commits allConsumed(), empty here.
  await consumer.commitAsync(callback);
  await consumer.commitAsync({ callback: async (answer) => callback(answer) });
  assert.deepEqual(seen, [
    ["{test-0=OffsetAndMetadata{offset=3, leaderEpoch=null, metadata=''}}", null],
    ['{}', null],
    ['{}', null],
    ['{}', null],
  ]);
  assert.equal((await consumer.committed(new Set([TP0]))).get(TP0)?.offset, 3n);
  // Java's commitAsync() is commitAsync(null); with offsets the FFI refuses it.
  await consumer.commitAsync(null);
  await assert.rejects(consumer.commitAsync({ offsets: new TopicPartitionMap(), callback: /** @type {never} */ (null) }), {
    constructor: TypeError,
    message: 'callback must not be null',
  });
  for (const value of [new KafkaError('the callback failed'), new TypeError('boom'), null, undefined]) {
    await assert.rejects(
      consumer.commitAsync({
        offsets: new TopicPartitionMap([[TP0, new OffsetAndMetadata(4)]]),
        callback: () => {
          throw value;
        },
      }),
      (error) => error === value,
    );
    await assert.rejects(consumer.commitAsync({ callback: () => Promise.reject(value) }), (error) => error === value);
  }
});
