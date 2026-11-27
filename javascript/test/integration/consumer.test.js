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
// metadata and the group metadata. Then the rebalance listeners: the two
// tests consumer-threading.md §31 asks for, a test per row of the window's
// table (the calls a listener makes while its report is pending), a test per
// row of the errors' table for a listener's error, each with a KafkaError
// and with another value, a listener without onPartitionsLost, and a
// listener that does not settle past a wakeup().

const assert = require('node:assert/strict');

const { KafkaError, Node, PartitionInfo, TopicPartition, TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');
const { WakeupError } = require('@confluentinc/kafka-javascript/common/errors');
const { ConcurrentModificationError } = require('@confluentinc/kafka-javascript');
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
    } finally {
      await consumer.close();
    }
  },
  120000,
);

// ---- the rebalance listeners ------------------------------------------------

/** @typedef {import('@confluentinc/kafka-javascript/consumer').ConsumerRebalanceListener} Listener */

const listenerTest = SKIP ? test.skip : test;

/**
 * A topic of one partition holding `count` records, and a consumer of a new
 * group subscribed to it with `listener`.
 *
 * @param {string} what
 * @param {Listener} listener
 * @param {number} [count]
 * @param {import('@confluentinc/kafka-javascript/consumer').ConsumerConfigs} [extra]
 * @returns {Promise<{ consumer: KafkaConsumer<Buffer, Buffer>, topic: string, tp: TopicPartition }>} the consumer, the topic and its partition
 */
async function subscribedWith(what, listener, count = 2, extra = {}) {
  const topic = freshTopic(what);
  await createTopic(BOOTSTRAP, topic);
  if (count > 0) {
    const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
    try {
      for (let i = 0; i < count; i++)
        await (await producer.send(new ProducerRecord({ topic, partition: 0, key: null, value: Buffer.from(`v${i}`) }))).get();
    } finally {
      await producer.close();
    }
  }
  const consumer = new KafkaConsumer({
    configs: {
      'bootstrap.servers': BOOTSTRAP,
      'group.protocol': 'consumer',
      'group.id': `${topic}-group`,
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': false,
      ...extra,
    },
  });
  await consumer.subscribe({ topics: [topic], callback: listener });
  return { consumer, topic, tp: new TopicPartition({ topic, partition: 0 }) };
}

/**
 * Polls until `done()` holds, a poll never awaited past 400 ms at a time, so
 * a poll a listener holds pending does not stop the loop; the polls still
 * pending are returned.
 *
 * @param {KafkaConsumer<Buffer, Buffer>} consumer
 * @param {() => boolean} done
 * @returns {Promise<Promise<unknown>[]>} the polls made, each settling or pending
 */
async function pollWhile(consumer, done) {
  /** @type {Promise<unknown>[]} */
  const polls = [];
  const deadline = Date.now() + 30000;
  while (!done()) {
    assert.ok(Date.now() < deadline, 'the listener never ran');
    const poll = consumer.poll({ timeout: 200 });
    // Each poll's outcome is read by the test that wants it.
    poll.catch(() => {});
    polls.push(poll);
    await Promise.race([poll.catch(() => {}), new Promise((resolve) => setTimeout(resolve, 400))]);
  }
  return polls;
}

/**
 * Closes `consumer` at once, whatever it answers.
 *
 * @param {KafkaConsumer<Buffer, Buffer>} consumer
 */
async function closeQuietly(consumer) {
  await consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) }).catch(() => {});
}

const NOT_SAFE = 'KafkaConsumer is not safe for multi-threaded access.';

listenerTest(
  'a commitSync() in onPartitionsRevoked succeeds (consumer-threading.md §31)',
  async () => {
    /** @type {unknown[]} */
    const revoked = [];
    let tp = /** @type {TopicPartition | null} */ (null);
    const { consumer, tp: partition } = await subscribedWith('revoke-commit', {
      async onPartitionsRevoked({ partitions }) {
        if (tp === null || !partitions.has(tp)) return;
        const offsets = new TopicPartitionMap();
        offsets.set(tp, new OffsetAndMetadata({ offset: 1n, leaderEpoch: null, metadata: 'revoked' }));
        // Through the consumer, as Java's listener calls it: the window sends it to the handle.
        await consumer.commitSync({ offsets });
        revoked.push('committed');
      },
      onPartitionsAssigned() {},
    });
    tp = partition;
    try {
      await pollWhile(consumer, () => consumer.assignment().has(partition));
      await consumer.unsubscribe();
      assert.deepEqual(revoked, ['committed']);
      const committed = await consumer.committed(new Set([partition]));
      assert.equal(committed.get(partition)?.offset, 1n);
      assert.equal(committed.get(partition)?.metadata, 'revoked');
    } finally {
      await closeQuietly(consumer);
    }
  },
  120000,
);

listenerTest(
  "poll() stays pending until the listener's Promise settles (consumer-threading.md §31)",
  async () => {
    /** @type {() => void} */
    let release = () => {};
    const gate = new Promise((resolve) => (release = () => resolve(undefined)));
    let entered = false;
    const { consumer, tp } = await subscribedWith('gate', {
      onPartitionsRevoked() {},
      onPartitionsAssigned({ partitions }) {
        if (partitions.size() === 0) return undefined;
        entered = true;
        return gate;
      },
    });
    try {
      const polls = await pollWhile(consumer, () => entered);
      const delivering = polls[polls.length - 1];
      let settled = false;
      void delivering.then(
        () => (settled = true),
        () => (settled = true),
      );
      await new Promise((resolve) => setTimeout(resolve, 1500));
      // The rebalance waits for the listener: the poll that delivered it is
      // pending, and the reconciliation has not ended, so the assignment the
      // listener got is the one the window reads.
      assert.equal(settled, false);
      assert.ok(consumer.assignment().has(tp));
      release();
      await delivering;
      assert.equal(settled, true);
      // The consumer is its own again: a poll goes to it.
      await consumer.poll({ timeout: 0 });
    } finally {
      release();
      await closeQuietly(consumer);
    }
  },
  120000,
);

/**
 * Runs `body` inside onPartitionsAssigned, once the topic's partition is
 * assigned, its report pending, and returns what `body` returned, with the
 * poll that delivered it.
 *
 * @template T
 * @param {string} what
 * @param {(consumer: KafkaConsumer<Buffer, Buffer>, tp: TopicPartition, topic: string) => Promise<T>} body
 * @returns {Promise<{ result: T, delivering: Promise<unknown>, consumer: KafkaConsumer<Buffer, Buffer> }>} its result
 */
async function insideListener(what, body) {
  /** @type {{ result: T } | null} */
  let out = null;
  /** @type {KafkaConsumer<Buffer, Buffer> | null} */
  let self = null;
  /** @type {TopicPartition | null} */
  let partition = null;
  let topicName = '';
  const { consumer, tp, topic } = await subscribedWith(what, {
    onPartitionsRevoked() {},
    async onPartitionsAssigned({ partitions }) {
      if (self === null || partition === null || !partitions.has(partition)) return;
      out = { result: await body(self, partition, topicName) };
    },
  });
  self = consumer;
  partition = tp;
  topicName = topic;
  const polls = await pollWhile(consumer, () => out !== null);
  const done = /** @type {{ result: T }} */ (/** @type {unknown} */ (out));
  return { result: done.result, delivering: polls[polls.length - 1], consumer };
}

listenerTest(
  "the window: inside a listener, the ConsumerHandle's waiting calls go through it",
  async () => {
    const { result, consumer } = await insideListener('window-cb', async (c, tp, topic) => {
      const offsets = new TopicPartitionMap();
      offsets.set(tp, new OffsetAndMetadata({ offset: 1n, leaderEpoch: null, metadata: '' }));
      const position = await c.position(tp);
      const positionTimed = await c.position({ partition: tp, timeout: 10000 });
      await c.seek({ partition: tp, offset: 1 });
      await c.seek({ partition: tp, offsetAndMetadata: new OffsetAndMetadata({ offset: 2n, leaderEpoch: null, metadata: '' }) });
      await c.seekToBeginning([tp]);
      await c.seekToEnd([tp]);
      await c.pause([tp]);
      await c.resume([tp]);
      await c.commitSync();
      await c.commitSync(10000);
      await c.commitSync(offsets);
      await c.commitSync({ offsets, timeout: 10000 });
      await c.commitAsync();
      const committed = await c.committed(new Set([tp]));
      const committedTimed = await c.committed({ partitions: new Set([tp]), timeout: 10000 });
      const partitionsFor = await c.partitionsFor(topic);
      const partitionsForTimed = await c.partitionsFor({ topic, timeout: 10000 });
      const listTopics = await c.listTopics();
      const listTopicsTimed = await c.listTopics(10000);
      const times = new TopicPartitionMap();
      times.set(tp, 0n);
      const forTimes = await c.offsetsForTimes(times);
      const forTimesTimed = await c.offsetsForTimes({ timestampsToSearch: times, timeout: 10000 });
      const beginning = await c.beginningOffsets([tp]);
      const beginningTimed = await c.beginningOffsets({ partitions: [tp], timeout: 10000 });
      const end = await c.endOffsets([tp]);
      const endTimed = await c.endOffsets({ partitions: [tp], timeout: 10000 });
      const lag = await c.currentLag(tp);
      return {
        positions: [position, positionTimed],
        committed: [committed.get(tp)?.offset, committedTimed.get(tp)?.offset],
        partitionsFor: [partitionsFor.length, partitionsForTimed.length],
        listTopics: [listTopics.has(topic), listTopicsTimed.has(topic)],
        forTimes: [forTimes.get(tp)?.offset(), forTimesTimed.get(tp)?.offset()],
        offsets: [beginning.get(tp), beginningTimed.get(tp), end.get(tp), endTimed.get(tp)],
        // Null until a fetch saw the log's end: either answers the call.
        lag: lag === null || typeof lag === 'bigint',
      };
    });
    try {
      assert.deepEqual(result, {
        positions: [0n, 0n],
        committed: [1n, 1n],
        partitionsFor: [1, 1],
        listTopics: [true, true],
        forTimes: [0n, 0n],
        offsets: [0n, 0n, 2n, 2n],
        lag: true,
      });
    } finally {
      await closeQuietly(consumer);
    }
  },
  120000,
);

listenerTest(
  "the window: inside a listener, assignment(), paused(), groupMetadata() and wakeup() go through the handle's sync forms",
  async () => {
    const { result, delivering, consumer } = await insideListener('window-sync', async (c, tp) => {
      await c.pause([tp]);
      const read = {
        assignment: [...c.assignment()].map(String),
        paused: [...c.paused()].map(String),
        groupId: c.groupMetadata().groupId(),
      };
      await c.resume([tp]);
      // The poll that runs this listener wakes once it returns.
      c.wakeup();
      return read;
    });
    try {
      assert.deepEqual(result.assignment, [...consumer.assignment()].map(String));
      assert.deepEqual(result.paused, result.assignment);
      assert.equal(result.groupId, consumer.groupMetadata().groupId());
      await assert.rejects(delivering, (error) => error instanceof WakeupError);
    } finally {
      await closeQuietly(consumer);
    }
  },
  120000,
);

listenerTest(
  'the window: inside a listener, metrics() goes to the consumer, which answers',
  async () => {
    const { result, consumer } = await insideListener('window-metrics', async (c) => c.metrics().size());
    try {
      assert.ok(result > 0);
    } finally {
      await closeQuietly(consumer);
    }
  },
  120000,
);

listenerTest(
  "the window: inside a listener, the calls the handle doesn't have go to the consumer, which refuses them",
  async () => {
    const { result, consumer } = await insideListener('window-refused', async (c, tp, topic) => {
      /** @type {Record<string, unknown>} */
      const refused = {};
      /** @type {Array<[string, () => unknown]>} */
      const calls = [
        ['poll', () => c.poll(0)],
        ['subscribe', () => c.subscribe([topic])],
        ['assign', () => c.assign([tp])],
        ['unsubscribe', () => c.unsubscribe()],
        ['close', () => c.close()],
        ['subscription', () => c.subscription()],
      ];
      for (const [name, call] of calls) {
        try {
          await call();
          refused[name] = 'answered';
        } catch (error) {
          refused[name] = error instanceof ConcurrentModificationError ? error.message : String(error);
        }
      }
      return refused;
    });
    try {
      assert.deepEqual(result, {
        poll: NOT_SAFE,
        subscribe: NOT_SAFE,
        assign: NOT_SAFE,
        unsubscribe: NOT_SAFE,
        close: NOT_SAFE,
        subscription: NOT_SAFE,
      });
    } finally {
      await closeQuietly(consumer);
    }
  },
  120000,
);

/** A listener's error: a KafkaError, and another value. */
const LISTENER_ERRORS = () => [new KafkaError('the listener failed'), new TypeError('boom')];

listenerTest(
  "a listener's error from poll(): a KafkaError as itself, another value inside KafkaError('User rebalance callback throws an error')",
  async () => {
    for (const value of LISTENER_ERRORS()) {
      const { consumer } = await subscribedWith('error-poll', {
        onPartitionsRevoked() {},
        onPartitionsAssigned({ partitions }) {
          if (partitions.size() > 0) throw value;
        },
      });
      try {
        const deadline = Date.now() + 30000;
        let error = /** @type {unknown} */ (null);
        while (error === null) {
          assert.ok(Date.now() < deadline, 'poll() never rejected');
          await consumer.poll({ timeout: 200 }).catch((e) => (error = e));
        }
        if (value instanceof KafkaError) {
          assert.equal(error, value);
        } else {
          assert.ok(error instanceof KafkaError);
          assert.equal(error.message, 'User rebalance callback throws an error');
          assert.equal(error.cause, value);
        }
      } finally {
        await closeQuietly(consumer);
      }
    }
  },
  180000,
);

listenerTest(
  "a listener's error from unsubscribe(), as from poll(), and from close() inside KafkaError('Failed to close kafka consumer')",
  async () => {
    for (const value of LISTENER_ERRORS()) {
      let fail = false;
      const { consumer, tp, topic } = await subscribedWith('error-unsubscribe', {
        async onPartitionsRevoked() {
          if (fail) throw value;
        },
        onPartitionsAssigned() {},
      });
      try {
        await pollWhile(consumer, () => consumer.assignment().has(tp));
        fail = true;
        const fromUnsubscribe = await consumer.unsubscribe().then(
          () => null,
          (e) => e,
        );
        if (value instanceof KafkaError) {
          assert.equal(fromUnsubscribe, value);
        } else {
          assert.ok(fromUnsubscribe instanceof KafkaError);
          assert.equal(fromUnsubscribe.message, 'User rebalance callback throws an error');
          assert.equal(fromUnsubscribe.cause, value);
        }
        // Subscribed again, the revoke on close fails the same way.
        await consumer.subscribe({
          topics: [topic],
          callback: { onPartitionsRevoked: () => Promise.reject(value), onPartitionsAssigned() {} },
        });
        await pollWhile(consumer, () => consumer.assignment().has(tp));
        const fromClose = await consumer.close().then(
          () => null,
          (e) => e,
        );
        assert.ok(fromClose instanceof KafkaError);
        assert.equal(fromClose.message, 'Failed to close kafka consumer');
        if (value instanceof KafkaError) {
          assert.equal(fromClose.cause, value);
        } else {
          // Java's maybeWrapAsKafkaException(e): a KafkaError built from the
          // value alone, its message the value's toString().
          const middle = fromClose.cause;
          assert.ok(middle instanceof KafkaError);
          assert.equal(middle.message, 'TypeError: boom');
          assert.equal(middle.cause, value);
        }
      } finally {
        await closeQuietly(consumer);
      }
    }
  },
  240000,
);

listenerTest(
  "a listener without onPartitionsLost: Java's default calls onPartitionsRevoked with the partitions lost",
  async () => {
    /** @type {Array<[string, string[]]>} */
    const seen = [];
    const { consumer, tp } = await subscribedWith(
      'lost',
      {
        onPartitionsRevoked({ partitions }) {
          seen.push(['revoked', [...partitions].map(String)]);
        },
        onPartitionsAssigned({ partitions }) {
          seen.push(['assigned', [...partitions].map(String)]);
        },
      },
      1,
      // A member that does not poll for longer than this goes stale: its
      // next poll loses its partitions (Java's transitionToStale).
      { 'max.poll.interval.ms': 2000 },
    );
    try {
      await pollWhile(consumer, () => consumer.assignment().has(tp));
      await new Promise((resolve) => setTimeout(resolve, 4000));
      await pollWhile(consumer, () => seen.some(([what]) => what === 'revoked'));
      assert.deepEqual(seen.slice(0, 2), [
        ['assigned', [String(tp)]],
        ['revoked', [String(tp)]],
      ]);
    } finally {
      await closeQuietly(consumer);
    }
  },
  120000,
);

listenerTest(
  'a listener that does not settle holds poll() past a wakeup(), until it settles',
  async () => {
    /** @type {() => void} */
    let release = () => {};
    const gate = new Promise((resolve) => (release = () => resolve(undefined)));
    let entered = false;
    const { consumer } = await subscribedWith('wakeup', {
      onPartitionsRevoked() {},
      onPartitionsAssigned({ partitions }) {
        if (partitions.size() === 0) return undefined;
        entered = true;
        return gate;
      },
    });
    try {
      const polls = await pollWhile(consumer, () => entered);
      const delivering = polls[polls.length - 1];
      let settled = false;
      void delivering.then(
        () => (settled = true),
        () => (settled = true),
      );
      // No timeout, and wakeup() doesn't end it.
      consumer.wakeup();
      await new Promise((resolve) => setTimeout(resolve, 1500));
      assert.equal(settled, false);
      release();
      // The wakeup is the delivering poll's, once the listener settled.
      await assert.rejects(delivering, (error) => error instanceof WakeupError);
      await consumer.poll({ timeout: 0 });
    } finally {
      release();
      await closeQuietly(consumer);
    }
  },
  120000,
);
