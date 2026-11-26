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

// The consumer against a broker: records the JavaScript producer wrote, read
// back by the JavaScript consumer, by subscribe and by assign, with their
// keys, values, headers, offsets and timestamps; the commits read back by
// committed(), the seeks and positions, the offset lookups, the partition
// metadata and the group metadata.

const assert = require('node:assert/strict');

const { Node, PartitionInfo, TopicPartition, TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');
const { Headers, RecordHeader } = require('@confluentinc/kafka-javascript/common/header');
const { TimestampType } = require('@confluentinc/kafka-javascript/common/record');
const {
  CloseOptions,
  ConsumerRecord,
  KafkaConsumer,
  OffsetAndMetadata,
  OffsetAndTimestamp,
} = require('@confluentinc/kafka-javascript/consumer');
const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
const { createTopic, waitForBroker } = require('./_fetch_records');

/** @typedef {import('@confluentinc/kafka-javascript/consumer').ConsumerRecords<Buffer, Buffer>} Records */

const BOOTSTRAP = /** @type {string} */ (process.env.KAFKA_BOOTSTRAP_SERVERS);
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to consume from';
if (SKIP) console.log(`skipping the consumer integration tests: ${SKIP}`);

// The broker `make test-integration` has just started may not listen yet, and
// this file may be the first to run.
beforeAll(async () => {
  if (!SKIP) await waitForBroker(BOOTSTRAP);
}, 90_000);

let topicCount = 0;
/** @param {string} what */
function freshTopic(what) {
  return `js-it-consumer-${what}-${process.pid}-${Date.now()}-${topicCount++}`;
}

/**
 * Writes three records to partition 0 of `topic`: the second with a null key,
 * the third with a null value; each with headers.
 *
 * @param {string} topic
 * @returns {Promise<bigint[]>} their timestamps
 */
async function produce(topic) {
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
  try {
    const timestamps = [1700000000000n, 1700000000001n, 1700000000002n];
    const records = [
      { key: Buffer.from('k0'), value: Buffer.from('v0'), headers: [new RecordHeader({ key: 'h', value: Buffer.from('x') })] },
      {
        key: null,
        value: Buffer.from('v1'),
        headers: [new RecordHeader({ key: 'h', value: null }), new RecordHeader({ key: 'i', value: Buffer.from('y') })],
      },
      { key: Buffer.from('k2'), value: null, headers: [] },
    ];
    for (const [i, r] of records.entries()) {
      const future = await producer.send(new ProducerRecord({ topic, partition: 0, timestamp: timestamps[i], ...r }));
      await future.get();
    }
    return timestamps;
  } finally {
    await producer.close();
  }
}

/**
 * Polls until `count` records came back, or 30 s passed.
 *
 * @param {KafkaConsumer} consumer
 * @param {number} count
 * @returns {Promise<ConsumerRecord<Buffer, Buffer>[]>} the records
 */
async function pollFor(consumer, count) {
  /** @type {ConsumerRecord<Buffer, Buffer>[]} */
  const out = [];
  const deadline = Date.now() + 30000;
  while (out.length < count && Date.now() < deadline) {
    /** @type {Records} */
    const records = await consumer.poll({ timeout: 500 });
    out.push(...records);
  }
  return out;
}

/**
 * The three records as `produce` wrote them.
 *
 * @param {ConsumerRecord<Buffer, Buffer>[]} records
 * @param {string} topic
 * @param {bigint[]} timestamps
 */
function checkRecords(records, topic, timestamps) {
  assert.equal(records.length, 3);
  for (const [i, record] of records.entries()) {
    assert.ok(record instanceof ConsumerRecord);
    assert.ok(Object.isFrozen(record));
    assert.equal(record.topic, topic);
    assert.equal(record.partition, 0);
    assert.equal(record.offset, BigInt(i));
    assert.equal(record.timestamp, timestamps[i]);
    assert.equal(record.timestampType, TimestampType.CREATE_TIME);
    assert.ok(record.headers instanceof Headers);
  }
  const [r0, r1, r2] = records;
  assert.ok(Buffer.isBuffer(r0.key) && Buffer.isBuffer(r0.value));
  assert.deepEqual([r0.key, r0.value], [Buffer.from('k0'), Buffer.from('v0')]);
  assert.equal(r0.serializedKeySize, 2);
  assert.equal(r0.serializedValueSize, 2);
  assert.deepEqual(
    r0.headers.toArray().map((h) => [h.key(), h.value()]),
    [['h', Buffer.from('x')]],
  );
  assert.equal(r1.key, null);
  assert.equal(r1.serializedKeySize, -1);
  assert.deepEqual(r1.value, Buffer.from('v1'));
  assert.deepEqual(
    r1.headers.toArray().map((h) => [h.key(), h.value()]),
    [
      ['h', null],
      ['i', Buffer.from('y')],
    ],
  );
  assert.deepEqual(r2.key, Buffer.from('k2'));
  assert.equal(r2.value, null);
  assert.equal(r2.serializedValueSize, -1);
  assert.equal(r2.headers.toArray().length, 0);
  // A record's headers are its own: a change touches only that record.
  r0.headers.add({ key: 'added', value: null });
  assert.equal(r0.headers.toArray().length, 2);
  assert.equal(r1.headers.toArray().length, 2);
}

(SKIP ? test.skip : test)(
  'the consumer reads back what the producer wrote, by subscribe',
  async () => {
    const topic = freshTopic('subscribe');
    await createTopic(BOOTSTRAP, topic);
    const timestamps = await produce(topic);
    const consumer = new KafkaConsumer({
      configs: {
        'bootstrap.servers': BOOTSTRAP,
        'group.protocol': 'consumer',
        'group.id': `${topic}-group`,
        'auto.offset.reset': 'earliest',
      },
    });
    try {
      await consumer.subscribe({ topics: [topic] });
      const records = await pollFor(consumer, 3);
      checkRecords(records, topic, timestamps);
      assert.deepEqual([...consumer.assignment()].map(String), [`${topic}-0`]);
    } finally {
      await consumer.close();
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'the consumer reads back what the producer wrote, by assign, with the next offsets of each poll',
  async () => {
    const topic = freshTopic('assign');
    await createTopic(BOOTSTRAP, topic);
    const timestamps = await produce(topic);
    const consumer = new KafkaConsumer({
      configs: { 'bootstrap.servers': BOOTSTRAP, 'group.protocol': 'consumer', 'auto.offset.reset': 'earliest' },
    });
    try {
      const tp = new TopicPartition({ topic, partition: 0 });
      await consumer.assign({ partitions: [tp] });
      /** @type {ConsumerRecord<Buffer, Buffer>[]} */
      const records = [];
      let next = -1n;
      const deadline = Date.now() + 30000;
      while (records.length < 3 && Date.now() < deadline) {
        /** @type {Records} */
        const polled = await consumer.poll({ timeout: 500 });
        records.push(...polled.records({ partition: tp }));
        const offset = polled.nextOffsets().get(tp);
        if (offset !== null) next = offset.offset;
      }
      checkRecords(records, topic, timestamps);
      assert.equal(next, 3n);
      // Paused, the partition returns nothing more.
      await consumer.pause({ partitions: [tp] });
      assert.equal((await consumer.poll({ timeout: 200 })).isEmpty(), true);
    } finally {
      await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'commitSync and commitAsync are read back by committed, with the leader epoch and metadata',
  async () => {
    const topic = freshTopic('commit');
    await createTopic(BOOTSTRAP, topic);
    await produce(topic);
    const group = `${topic}-group`;
    const consumer = new KafkaConsumer({
      configs: { 'bootstrap.servers': BOOTSTRAP, 'group.protocol': 'consumer', 'group.id': group, 'auto.offset.reset': 'earliest' },
    });
    const tp = new TopicPartition({ topic, partition: 0 });
    try {
      await consumer.assign({ partitions: [tp] });
      assert.equal((await consumer.committed({ partitions: [tp] })).get(tp), null);
      await consumer.commitSync({
        offsets: new TopicPartitionMap([[tp, new OffsetAndMetadata({ offset: 1n, leaderEpoch: 0, metadata: 'm' })]]),
      });
      const first = (await consumer.committed({ partitions: [tp], timeout: 10000 })).get(tp);
      assert.ok(first instanceof OffsetAndMetadata);
      assert.equal(first.offset, 1n);
      assert.equal(first.leaderEpoch, 0);
      assert.equal(first.metadata, 'm');
      // commitSync() and commitAsync() commit the consumed positions. The
      // poll starts at the committed offset 1, so two records are left.
      assert.equal((await pollFor(consumer, 2)).length, 2);
      await consumer.commitSync();
      assert.equal((await consumer.committed({ partitions: [tp] })).get(tp)?.offset, 3n);
      // A seek back to 1 makes 1 the position commitAsync() commits, an offset
      // other than the one committed, so the read fails if it commits nothing.
      await consumer.seek({ partition: tp, offset: 1 });
      await consumer.commitAsync();
      // commitAsync() settled once the core took the commit; the broker's
      // answer comes later, so the read waits on a deadline.
      const deadline = Date.now() + 30000;
      let offset = -1n;
      while (offset !== 1n && Date.now() < deadline) {
        offset = (await consumer.committed({ partitions: [tp] })).get(tp)?.offset ?? -1n;
      }
      assert.equal(offset, 1n);
      await consumer.commitSync({ offsets: new TopicPartitionMap([[tp, new OffsetAndMetadata(2)]]), timeout: 10000 });
      assert.equal((await consumer.committed({ partitions: [tp] })).get(tp)?.offset, 2n);
    } finally {
      await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'seek, seekToBeginning and seekToEnd move the position; the offset lookups and currentLag answer from the log',
  async () => {
    const topic = freshTopic('offsets');
    await createTopic(BOOTSTRAP, topic);
    const timestamps = await produce(topic);
    const consumer = new KafkaConsumer({
      configs: { 'bootstrap.servers': BOOTSTRAP, 'group.protocol': 'consumer', 'auto.offset.reset': 'earliest' },
    });
    const tp = new TopicPartition({ topic, partition: 0 });
    try {
      await consumer.assign({ partitions: [tp] });
      await consumer.seek({ partition: tp, offset: 1 });
      assert.equal(await consumer.position({ partition: tp }), 1n);
      const [record] = (await pollFor(consumer, 2)).slice(0, 1);
      assert.equal(record.offset, 1n);
      await consumer.seek({ partition: tp, offsetAndMetadata: new OffsetAndMetadata({ offset: 2n, leaderEpoch: 0, metadata: '' }) });
      assert.equal(await consumer.position({ partition: tp, timeout: 10000 }), 2n);
      await consumer.seekToBeginning({ partitions: [tp] });
      assert.equal(await consumer.position({ partition: tp }), 0n);
      await consumer.seekToEnd({ partitions: [tp] });
      assert.equal(await consumer.position({ partition: tp }), 3n);
      // With the position and the end offset known, the lag is known.
      await consumer.seek({ partition: tp, offset: 1 });
      const ends = await consumer.endOffsets({ partitions: [tp] });
      assert.equal(ends.get(tp), 3n);
      assert.equal(await consumer.currentLag({ topicPartition: tp }), 2n);
      const begins = await consumer.beginningOffsets({ partitions: [tp], timeout: 10000 });
      assert.equal(begins.size(), 1);
      assert.equal(begins.get(tp), 0n);
      assert.equal((await consumer.endOffsets({ partitions: [tp], timeout: 10000 })).get(tp), 3n);
      // The first record at or after each timestamp, and none past the last.
      const found = await consumer.offsetsForTimes({
        timestampsToSearch: new TopicPartitionMap([[tp, timestamps[1]]]),
      });
      const hit = found.get(tp);
      assert.ok(hit instanceof OffsetAndTimestamp);
      assert.equal(hit.offset(), 1n);
      assert.equal(hit.timestamp(), timestamps[1]);
      assert.equal(hit.leaderEpoch(), 0);
      const past = await consumer.offsetsForTimes({
        timestampsToSearch: new TopicPartitionMap([[tp, timestamps[2] + 1n]]),
        timeout: 10000,
      });
      assert.equal(past.size(), 1);
      assert.equal(past.get(tp), null);
    } finally {
      await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'partitionsFor and listTopics of a real topic, an empty list for a missing one',
  async () => {
    const topic = freshTopic('metadata');
    await createTopic(BOOTSTRAP, topic, { partitions: 2 });
    const consumer = new KafkaConsumer({
      configs: { 'bootstrap.servers': BOOTSTRAP, 'group.protocol': 'consumer', 'allow.auto.create.topics': false },
    });
    try {
      const partitions = await consumer.partitionsFor({ topic });
      assert.deepEqual(partitions.map((p) => p.partition()).sort(), [0, 1]);
      for (const p of partitions) {
        assert.ok(p instanceof PartitionInfo);
        assert.equal(p.topic(), topic);
        assert.ok(p.leader() instanceof Node);
        assert.equal(p.replicas()?.length, 1);
      }
      assert.equal((await consumer.partitionsFor({ topic, timeout: 10000 })).length, 2);
      assert.deepEqual(await consumer.partitionsFor({ topic: freshTopic('missing') }), []);
      const topics = await consumer.listTopics();
      assert.ok(topics instanceof Map);
      assert.equal(topics.get(topic)?.length, 2);
      assert.ok(topics.get(topic)?.every((p) => p instanceof PartitionInfo && p.topic() === topic));
      assert.ok((await consumer.listTopics({ timeout: 10000 })).has(topic));
    } finally {
      await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
    }
  },
  120000,
);

(SKIP ? test.skip : test)(
  'groupMetadata() before and after the member joins',
  async () => {
    const topic = freshTopic('group');
    await createTopic(BOOTSTRAP, topic);
    const group = `${topic}-group`;
    const consumer = new KafkaConsumer({
      configs: { 'bootstrap.servers': BOOTSTRAP, 'group.protocol': 'consumer', 'group.id': group },
    });
    try {
      const before = consumer.groupMetadata();
      assert.equal(before.groupId(), group);
      assert.equal(before.generationId(), -1);
      assert.equal(before.memberId(), '');
      await consumer.subscribe({ topics: [topic] });
      const deadline = Date.now() + 30000;
      while (consumer.assignment().size() === 0 && Date.now() < deadline) await consumer.poll({ timeout: 200 });
      const after = consumer.groupMetadata();
      assert.equal(after.groupId(), group);
      assert.ok(after.generationId() > 0);
      assert.notEqual(after.memberId(), '');
      assert.equal(after.groupInstanceId(), null);
      // enforceRebalance() only logs under the consumer protocol, and resolves.
      await consumer.enforceRebalance();
      await consumer.enforceRebalance({ reason: 'test' });
    } finally {
      await consumer.close();
    }
  },
  120000,
);
