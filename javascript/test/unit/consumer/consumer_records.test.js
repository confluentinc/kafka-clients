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

// Java's ConsumerRecordsTest, 7 of its 8 tests, and the binding's own checks.
// Not translated: testNextOffsetsLogsErrorPeriodicallyWhenConstructedWithDeprecatedConstructor,
// which tests the log line of the deprecated ConsumerRecords(Map) constructor,
// which is not translated. testRecordsAreImmutable asserts copies instead:
// every collection the class returns is a new object the caller may change
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family), so a
// change to one leaves the records unchanged. The two "nothing is logged"
// tests check that process.emitWarning is not called, as the binding logs
// nothing else.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { TopicPartition, TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');
const { Headers } = require('@confluentinc/kafka-javascript/common/header');
const { TimestampType } = require('@confluentinc/kafka-javascript/common/record');
const { ConsumerRecord, ConsumerRecords, OffsetAndMetadata } = require('@confluentinc/kafka-javascript/consumer');

/** @typedef {import('@confluentinc/kafka-javascript/consumer').ConsumerRecord<number, string>} Record */

// ConsumerRecords' constructor as its implementation takes its parameter, for
// the key sets it rejects, which no overload names.
const AnyForm = /** @type {new (options: unknown) => ConsumerRecords<unknown, unknown>} */ (/** @type {unknown} */ (ConsumerRecords));

/**
 * @param {number} recordSize
 * @param {number} partitionSize
 * @param {number} emptyPartitionIndex
 * @param {Iterable<string>} topics
 * @returns {ConsumerRecords<number, string>}
 */
function buildTopicTestRecords(recordSize, partitionSize, emptyPartitionIndex, topics) {
  /** @type {TopicPartitionMap<Record[]>} */
  const partitionToRecords = new TopicPartitionMap();
  /** @type {TopicPartitionMap<OffsetAndMetadata>} */
  const nextOffsets = new TopicPartitionMap();
  for (const topic of topics) {
    for (let i = 0; i < partitionSize; i++) {
      /** @type {Record[]} */
      const records = [];
      if (i !== emptyPartitionIndex) {
        for (let j = 0; j < recordSize; j++) {
          records.push(
            new ConsumerRecord({
              topic,
              partition: i,
              offset: j,
              timestamp: 0n,
              timestampType: TimestampType.CREATE_TIME,
              serializedKeySize: 0,
              serializedValueSize: 0,
              key: j,
              value: String(j),
              headers: new Headers(),
              leaderEpoch: null,
            }),
          );
        }
      }
      const tp = new TopicPartition({ topic, partition: i });
      partitionToRecords.set(tp, records);
      nextOffsets.set(tp, new OffsetAndMetadata({ offset: recordSize, leaderEpoch: null, metadata: '' }));
    }
  }
  return new ConsumerRecords({ records: partitionToRecords, nextOffsets });
}

/**
 * @param {Record} record
 * @param {number} emptyPartitionIndex
 */
function validateEmptyPartition(record, emptyPartitionIndex) {
  assert.notEqual(record.partition, emptyPartitionIndex, `Partition ${record.partition} is not empty`);
}

/**
 * @param {string} topic
 * @param {Record} record
 * @param {number} currentPartition
 * @param {number} recordCount
 * @param {number} recordSize
 */
function validateRecordPayload(topic, record, currentPartition, recordCount, recordSize) {
  assert.equal(record.topic, topic);
  assert.equal(record.partition, currentPartition);
  assert.equal(record.offset, BigInt(recordCount % recordSize));
  assert.equal(record.key, recordCount % recordSize);
  assert.equal(record.value, String(recordCount % recordSize));
}

/**
 * Runs `fn` and returns how many times it called process.emitWarning.
 *
 * @param {() => void} fn
 * @returns {number}
 */
function warnings(fn) {
  let count = 0;
  const original = process.emitWarning;
  process.emitWarning = () => {
    count += 1;
  };
  try {
    fn();
  } finally {
    process.emitWarning = original;
  }
  return count;
}

test('testIterator', () => {
  const topic = 'topic';
  const recordSize = 10;
  const partitionSize = 15;
  const emptyPartitionIndex = 3;
  const records = buildTopicTestRecords(recordSize, partitionSize, emptyPartitionIndex, [topic]);
  const iterator = records[Symbol.iterator]();

  let recordCount = 0;
  let partitionCount = 0;
  let currentPartition = -1;

  for (let next = iterator.next(); !next.done; next = iterator.next()) {
    const record = next.value;
    validateEmptyPartition(record, emptyPartitionIndex);

    // Check if we have moved to a new partition
    if (currentPartition !== record.partition) {
      // Increment the partition count as we have encountered a new partition
      partitionCount++;
      // Update the current partition to the new partition
      currentPartition = record.partition;
    }

    validateRecordPayload(topic, record, currentPartition, recordCount, recordSize);
    recordCount++;
  }

  // Including empty partition
  assert.equal(partitionCount + 1, partitionSize);
});

test('testRecordsByPartition', () => {
  const topics = ['topic1', 'topic2'];
  const recordSize = 3;
  const partitionSize = 5;
  const emptyPartitionIndex = 2;

  const consumerRecords = buildTopicTestRecords(recordSize, partitionSize, emptyPartitionIndex, topics);

  assert.equal(consumerRecords.nextOffsets().size(), partitionSize * topics.length);
  for (const topic of topics) {
    for (let partition = 0; partition < partitionSize; partition++) {
      const topicPartition = new TopicPartition({ topic, partition });
      const records = consumerRecords.records({ partition: topicPartition });

      if (partition === emptyPartitionIndex) {
        assert.equal(records.length, 0);
      } else {
        assert.equal(records.length, recordSize);
        const lastRecord = records[recordSize - 1];
        const expected = new OffsetAndMetadata({ offset: lastRecord.offset + 1n, leaderEpoch: lastRecord.leaderEpoch, metadata: '' });
        assert.ok(expected.equals(consumerRecords.nextOffsets().get(topicPartition)));
        for (let i = 0; i < records.length; i++) {
          validateRecordPayload(topic, records[i], partition, i, recordSize);
        }
      }
    }
  }
});

test('testRecordsByNullTopic', () => {
  /** @type {ConsumerRecords<number, string>} */
  const consumerRecords = ConsumerRecords.empty();
  // @ts-expect-error a null the types forbid: Java's records(String) checks the topic
  assert.throws(() => consumerRecords.records({ topic: null }), {
    constructor: IllegalArgumentError,
    message: 'Topic must be non-null.',
  });
});

test('testRecordsByTopic', () => {
  const topics = ['topic1', 'topic2', 'topic3', 'topic4'];
  const recordSize = 3;
  const partitionSize = 10;
  const emptyPartitionIndex = 6;
  const expectedTotalRecordSizeOfEachTopic = recordSize * (partitionSize - 1);

  const consumerRecords = buildTopicTestRecords(recordSize, partitionSize, emptyPartitionIndex, topics);

  assert.equal(consumerRecords.nextOffsets().size(), partitionSize * topics.length);

  for (const topic of topics) {
    const records = consumerRecords.records({ topic });
    let recordCount = 0;
    let partitionCount = 0;
    let currentPartition = -1;

    for (const record of records) {
      validateEmptyPartition(record, emptyPartitionIndex);

      // Check if we have moved to a new partition
      if (currentPartition !== record.partition) {
        // Increment the partition count as we have encountered a new partition
        partitionCount++;
        // Update the current partition to the new partition
        currentPartition = record.partition;
      }

      validateRecordPayload(topic, record, currentPartition, recordCount, recordSize);
      recordCount++;
    }

    // Including empty partition
    assert.equal(partitionCount + 1, partitionSize);
    assert.equal(recordCount, expectedTotalRecordSizeOfEachTopic);
  }
});

test('testRecordsAreImmutable, as copies: changing what the methods return leaves the records unchanged', () => {
  const topic = 'topic';
  const recordSize = 3;
  const partitionSize = 6;
  const emptyPartitionIndex = 2;
  const topicPartition = new TopicPartition({ topic, partition: 0 });
  const newRecord = new ConsumerRecord({
    topic,
    partition: 0,
    offset: 0,
    timestamp: 0n,
    timestampType: TimestampType.CREATE_TIME,
    serializedKeySize: 0,
    serializedValueSize: 0,
    key: 0,
    value: '0',
    headers: new Headers(),
    leaderEpoch: null,
  });
  const records = buildTopicTestRecords(recordSize, partitionSize, emptyPartitionIndex, [topic]);
  /** @type {ConsumerRecords<number, string>} */
  const emptyRecords = ConsumerRecords.empty();

  assert.equal(records.nextOffsets().size(), partitionSize);
  for (const [subject, count] of /** @type {Array<[ConsumerRecords<number, string>, number]>} */ ([
    [records, recordSize * (partitionSize - 1)],
    [emptyRecords, 0],
  ])) {
    subject.records({ partition: topicPartition }).push(newRecord);
    subject.partitions().add(new TopicPartition({ topic, partition: 99 }));
    subject.records({ topic }).pop();
    subject.nextOffsets().clear();
    assert.equal(subject.count(), count);
    assert.equal([...subject].length, count);
    assert.equal(subject.records({ topic }).length, count);
    assert.equal(subject.partitions().has(new TopicPartition({ topic, partition: 99 })), false);
  }
  assert.equal(records.nextOffsets().size(), partitionSize);
  assert.notEqual(records.records({ partition: topicPartition }), records.records({ partition: topicPartition }));
  assert.ok(Object.isFrozen(ConsumerRecords.EMPTY));
});

test('testNextOffsetsDoesNotLogErrorWhenConstructedWithNextOffsets', () => {
  const tp = new TopicPartition({ topic: 'topic', partition: 0 });
  const records = new TopicPartitionMap([[tp, [new ConsumerRecord({ topic: 'topic', partition: 0, offset: 0, key: 0, value: 'value' })]]]);
  const nextOffsets = new TopicPartitionMap([[tp, new OffsetAndMetadata(1n)]]);

  const logged = warnings(() => {
    const consumerRecords = new ConsumerRecords({ records, nextOffsets });
    assert.equal(consumerRecords.nextOffsets().size(), 1);
    const offset = consumerRecords.nextOffsets().get(tp);
    assert.ok(offset !== null && offset.equals(new OffsetAndMetadata(1n)));
  });
  assert.equal(logged, 0);
});

test('testNextOffsetsDoesNotLogErrorForEmptyRecords', () => {
  const logged = warnings(() => assert.equal(ConsumerRecords.empty().nextOffsets().size(), 0));
  assert.equal(logged, 0);
});

test("the deprecated { records } form is refused; nextOffsets is copied as Java's Map.copyOf", () => {
  const records = new TopicPartitionMap();
  assert.throws(() => new AnyForm({ records }), {
    constructor: IllegalArgumentError,
    message: 'ConsumerRecords() takes one of (records, nextOffsets); got (records)',
  });
  // @ts-expect-error a null the types forbid: Java's Map.copyOf dereferences it
  assert.throws(() => new ConsumerRecords({ records, nextOffsets: null }), {
    constructor: TypeError,
    message: 'nextOffsets must not be null',
  });
  const tp = new TopicPartition({ topic: 't', partition: 0 });
  // @ts-expect-error a null the types forbid: Java's Map.copyOf refuses a null value
  assert.throws(() => new ConsumerRecords({ records, nextOffsets: new TopicPartitionMap([[tp, null]]) }), {
    constructor: TypeError,
    message: 'nextOffsets must not contain null',
  });
  // @ts-expect-error a wrong argument: only a TopicPartitionMap is taken
  assert.throws(() => new ConsumerRecords({ records: new Map(), nextOffsets: records }), { constructor: TypeError });
  const nextOffsets = new TopicPartitionMap([[tp, new OffsetAndMetadata(5n)]]);
  const copied = new ConsumerRecords({ records, nextOffsets });
  nextOffsets.clear();
  assert.equal(copied.nextOffsets().size(), 1);
});

test('null records are kept, and a method that reads them throws', () => {
  // @ts-expect-error a null the types forbid: Java keeps the records' map as given
  const records = new ConsumerRecords({ records: null, nextOffsets: new TopicPartitionMap() });
  const tp = new TopicPartition({ topic: 't', partition: 0 });
  for (const read of [
    () => records.records({ partition: tp }),
    () => records.records({ topic: 't' }),
    () => records.partitions(),
    () => records.count(),
    () => records.isEmpty(),
    () => [...records],
  ]) {
    assert.throws(read, { constructor: TypeError, message: 'records must not be null' });
  }
  assert.equal(records.nextOffsets().size(), 0);
});

test('records takes its value bare, told apart by type; a partition with no list gives []', () => {
  const tp = new TopicPartition({ topic: 't', partition: 0 });
  const record = new ConsumerRecord({ topic: 't', partition: 0, offset: 0, key: 0, value: 'v' });
  const records = new ConsumerRecords({ records: new TopicPartitionMap([[tp, [record]]]), nextOffsets: new TopicPartitionMap() });
  assert.deepEqual(records.records(tp), [record]);
  assert.deepEqual(records.records('t'), [record]);
  assert.deepEqual(records.records(new TopicPartition({ topic: 't', partition: 1 })), []);
  assert.deepEqual(records.records('other'), []);
  // @ts-expect-error a wrong argument: a bare null fits neither form, as Java's records(null) is ambiguous
  assert.throws(() => records.records(null), {
    constructor: IllegalArgumentError,
    message: 'records() takes one of (partition), (topic); got null',
  });
  // @ts-expect-error a wrong argument: a bare number fits neither form
  assert.throws(() => records.records(1), {
    constructor: IllegalArgumentError,
    message: 'records() takes one of (partition), (topic); got a number',
  });
  assert.equal(records.isEmpty(), false);
  assert.equal(records.count(), 1);
  assert.deepEqual([...records.partitions()], [tp]);
  assert.deepEqual(records.records({ partition: null }), []);
  // @ts-expect-error a wrong argument: the partition is a TopicPartition
  assert.throws(() => records.records({ partition: 'x' }), { constructor: TypeError, message: 'partition must be a TopicPartition' });
  // @ts-expect-error a wrong argument: a plain object is no TopicPartition
  assert.throws(() => records.records({ partition: { topic: 't', partition: 0 } }), {
    constructor: TypeError,
    message: 'partition must be a TopicPartition',
  });
  // @ts-expect-error a wrong argument: a plain object is no TopicPartition
  assert.throws(() => records.records({ partition: { key: tp } }), {
    constructor: TypeError,
    message: 'partition must be a TopicPartition',
  });
});

test('isEmpty() is whether the map is empty, as Java reads it', () => {
  const tp = new TopicPartition({ topic: 't', partition: 0 });
  /** @type {ConsumerRecords<number, string>} */
  const records = new ConsumerRecords({ records: new TopicPartitionMap([[tp, []]]), nextOffsets: new TopicPartitionMap() });
  assert.equal(records.count(), 0);
  assert.equal(records.isEmpty(), false);
  assert.equal(ConsumerRecords.empty().isEmpty(), true);
  assert.equal(ConsumerRecords.empty(), ConsumerRecords.EMPTY);
  // @ts-expect-error a wrong argument: count() only reads
  assert.throws(() => records.count(1), { constructor: TypeError, message: 'count() only reads: it takes no argument' });
});
