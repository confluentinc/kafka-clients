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

// KafkaConsumerTest (Apache Kafka 4.4.0) cases Java scripts through a
// MockClient, run against a broker: the broker's answers stand for Java's
// prepared ones, so the offsets are the real log's (Java's MockClient invents
// 20, 40, 50, 90 or 539), and Java's prepared OffsetFetch is a commit made
// first by another consumer of the group. Each runs the consumer protocol's
// arm, as test/unit/consumer/kafka_consumer.test.js says.
//
// The 13 cases: testMissingOffsetNoResetPolicy, testResetToCommittedOffset,
// testResetUsingAutoResetPolicy, testResetUsingDurationBasedAutoResetPolicy,
// testOffsetIsValidAfterSeek, testCommitsFetchedDuringAssign,
// testNoCommittedOffsets, testManualAssignmentChangeWithAutoCommitEnabled,
// testManualAssignmentChangeWithAutoCommitDisabled,
// testOffsetOfPausedPartitions, testListOffsetShouldUpdateSubscriptions,
// verifyNoCoordinatorLookupForManualAssignmentWithSeek and
// testPollSendsRequestToJoin. Not here yet: testSubscriptionOnInvalidTopic,
// which subscribes with a rebalance listener, for want of the listeners.

const assert = require('node:assert/strict');

const { TopicPartition, TopicPartitionMap, TopicPartitionSet } = require('@confluentinc/kafka-javascript/common');
const { CloseOptions, KafkaConsumer, NoOffsetForPartitionError, OffsetAndMetadata } = require('@confluentinc/kafka-javascript/consumer');
const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
const { createTopic, waitForBroker } = require('./_fetch_records');

/** @typedef {import('@confluentinc/kafka-javascript/consumer').ConsumerConfigs} ConsumerConfigs */

const BOOTSTRAP = /** @type {string} */ (process.env.KAFKA_BOOTSTRAP_SERVERS);
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to consume from';
if (SKIP) console.log(`skipping the KafkaConsumerTest broker cases: ${SKIP}`);
const it = SKIP ? test.skip : test;

// How long a case waits on the broker for what it expects.
const DEADLINE_MS = 30000;
const TIMEOUT = 120000;

// The broker `make test-integration` has just started may not listen yet, and
// this file may be the first to run.
beforeAll(async () => {
  if (!SKIP) await waitForBroker(BOOTSTRAP);
}, 90_000);

let count = 0;
/** @param {string} what */
function freshName(what) {
  return `js-it-kc-${what}-${process.pid}-${Date.now()}-${count++}`;
}

/**
 * A consumer's configuration: its own group, from the earliest offset, no
 * auto-commit unless `extra` says so.
 *
 * @param {ConsumerConfigs} [extra] more keys, a null one removing the key
 * @returns {ConsumerConfigs} the configuration
 */
function configs(extra = {}) {
  return {
    'bootstrap.servers': BOOTSTRAP,
    'group.protocol': 'consumer',
    'group.id': freshName('group'),
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': false,
    ...extra,
  };
}

/**
 * A new topic of `partitions` partitions, each holding `values` in order.
 *
 * @param {string[]} values
 * @param {number} [partitions]
 * @returns {Promise<string>} the topic
 */
async function topicWith(values, partitions = 1) {
  const topic = freshName('topic');
  await createTopic(BOOTSTRAP, topic, { partitions });
  const producer = new KafkaProducer({ configs: { 'bootstrap.servers': BOOTSTRAP, acks: 'all' } });
  try {
    for (let partition = 0; partition < partitions; partition++) {
      for (const value of values) await producer.send(new ProducerRecord({ topic, partition, key: null, value: Buffer.from(value) }));
    }
    await producer.flush();
  } finally {
    await producer.close();
  }
  return topic;
}

/**
 * Commits `offsets` for the group of `cfg` from another consumer: Java's
 * prepared OffsetFetch answer.
 *
 * @param {ConsumerConfigs} cfg
 * @param {Array<[TopicPartition, bigint]>} offsets
 */
async function commitFor(cfg, offsets) {
  const committer = new KafkaConsumer({ configs: cfg });
  try {
    await committer.assign({ partitions: offsets.map(([tp]) => tp) });
    const map = new TopicPartitionMap();
    for (const [tp, offset] of offsets) map.set(tp, new OffsetAndMetadata(offset));
    await committer.commitSync({ offsets: map });
  } finally {
    await committer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
  }
}

/**
 * Closes `consumer` at once, as Java's tests close theirs.
 *
 * @param {KafkaConsumer} consumer
 */
function closeNow(consumer) {
  return consumer.close({ option: CloseOptions.timeout({ timeout: 0 }) });
}

/**
 * Java's assertPollEventuallyThrows: poll(ZERO) until it rejects.
 *
 * @param {KafkaConsumer} consumer
 * @returns {Promise<unknown>} what it rejected with
 */
async function pollEventuallyRejects(consumer) {
  const deadline = Date.now() + DEADLINE_MS;
  for (;;) {
    assert.ok(Date.now() < deadline, 'poll() never rejected');
    try {
      await consumer.poll({ timeout: 0 });
    } catch (error) {
      return error;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

/**
 * Polls until a poll returns records.
 *
 * @param {KafkaConsumer} consumer
 * @returns {Promise<import('@confluentinc/kafka-javascript/consumer').ConsumerRecords<Buffer, Buffer>>} the records
 */
async function pollRecords(consumer) {
  const deadline = Date.now() + DEADLINE_MS;
  for (;;) {
    assert.ok(Date.now() < deadline, 'no records');
    const records = await consumer.poll({ timeout: 500 });
    if (!records.isEmpty()) return records;
  }
}

/**
 * A committed offset's value, failing when there is none.
 *
 * @param {KafkaConsumer} consumer
 * @param {TopicPartition} tp
 * @returns {Promise<bigint>} the offset
 */
async function committedOffset(consumer, tp) {
  const committed = (await consumer.committed({ partitions: [tp] })).get(tp);
  assert.ok(committed !== null, `no committed offset for ${tp}`);
  return committed.offset;
}

it(
  'testMissingOffsetNoResetPolicy',
  async () => {
    const topic = await topicWith(['a']);
    const tp = new TopicPartition({ topic, partition: 0 });
    const consumer = new KafkaConsumer({ configs: configs({ 'auto.offset.reset': 'none' }) });
    try {
      await consumer.assign({ partitions: [tp] });
      const error = await pollEventuallyRejects(consumer);
      assert.ok(error instanceof NoOffsetForPartitionError, String(error));
      assert.deepEqual([...error.partitions()].map(String), [String(tp)]);
    } finally {
      await closeNow(consumer);
    }
  },
  TIMEOUT,
);

it(
  'testResetToCommittedOffset',
  async () => {
    const topic = await topicWith(['a', 'b', 'c']);
    const tp = new TopicPartition({ topic, partition: 0 });
    const cfg = configs({ 'auto.offset.reset': 'none' });
    await commitFor(cfg, [[tp, 2n]]);
    const consumer = new KafkaConsumer({ configs: cfg });
    try {
      await consumer.assign({ partitions: [tp] });
      await consumer.poll({ timeout: 0 });
      assert.equal(await consumer.position({ partition: tp }), 2n);
    } finally {
      await closeNow(consumer);
    }
  },
  TIMEOUT,
);

// by_duration:PT1H: the three records were produced just now, so the first
// offset at or after one hour ago is the first record's.
for (const [name, strategy, expected] of /** @type {const} */ ([
  ['testResetUsingAutoResetPolicy', 'latest', 3n],
  ['testResetUsingDurationBasedAutoResetPolicy', 'by_duration:PT1H', 0n],
])) {
  it(
    name,
    async () => {
      const topic = await topicWith(['a', 'b', 'c']);
      const tp = new TopicPartition({ topic, partition: 0 });
      const consumer = new KafkaConsumer({ configs: configs({ 'auto.offset.reset': strategy }) });
      try {
        await consumer.assign({ partitions: [tp] });
        await consumer.poll({ timeout: 0 });
        assert.equal(await consumer.position({ partition: tp }), expected);
      } finally {
        await closeNow(consumer);
      }
    },
    TIMEOUT,
  );
}

it(
  'testOffsetIsValidAfterSeek',
  async () => {
    const topic = await topicWith(['a', 'b', 'c']);
    const tp = new TopicPartition({ topic, partition: 0 });
    const consumer = new KafkaConsumer({ configs: configs({ 'auto.offset.reset': 'latest' }) });
    try {
      await consumer.assign({ partitions: [tp] });
      await consumer.seek({ partition: tp, offset: 1 });
      await consumer.poll({ timeout: 0 });
      assert.equal(await consumer.position({ partition: tp }), 1n);
    } finally {
      await closeNow(consumer);
    }
  },
  TIMEOUT,
);

it(
  'testCommitsFetchedDuringAssign',
  async () => {
    const topic = await topicWith(['a', 'b', 'c'], 2);
    const tp0 = new TopicPartition({ topic, partition: 0 });
    const tp1 = new TopicPartition({ topic, partition: 1 });
    const cfg = configs();
    await commitFor(cfg, [
      [tp0, 1n],
      [tp1, 2n],
    ]);
    const consumer = new KafkaConsumer({ configs: cfg });
    try {
      await consumer.assign({ partitions: [tp0] });
      assert.equal(await committedOffset(consumer, tp0), 1n);
      // A new partition added to the assignment: its committed offset is fetched too.
      await consumer.assign({ partitions: [tp0, tp1] });
      assert.equal(await committedOffset(consumer, tp0), 1n);
      assert.equal(await committedOffset(consumer, tp1), 2n);
    } finally {
      await closeNow(consumer);
    }
  },
  TIMEOUT,
);

it(
  'testNoCommittedOffsets',
  async () => {
    const topic = await topicWith(['a'], 2);
    const tp0 = new TopicPartition({ topic, partition: 0 });
    const tp1 = new TopicPartition({ topic, partition: 1 });
    const cfg = configs();
    await commitFor(cfg, [[tp0, 1n]]);
    const consumer = new KafkaConsumer({ configs: cfg });
    try {
      await consumer.assign({ partitions: [tp0, tp1] });
      const committed = await consumer.committed({ partitions: new TopicPartitionSet([tp0, tp1]) });
      assert.equal(committed.size(), 2);
      assert.equal(committed.get(tp0)?.offset, 1n);
      assert.equal(committed.get(tp1), null);
    } finally {
      await closeNow(consumer);
    }
  },
  TIMEOUT,
);

// Java's assign() auto-commits the consumed positions once the auto-commit
// interval has elapsed (CommitRequestManager#updateTimerAndMaybeCommit), as
// Java's MockTime has it; a short interval and a wait on a deadline stand for
// that here.
for (const [name, autoCommit] of /** @type {const} */ ([
  ['testManualAssignmentChangeWithAutoCommitEnabled', true],
  ['testManualAssignmentChangeWithAutoCommitDisabled', false],
])) {
  it(
    name,
    async () => {
      const topic = await topicWith(['a']);
      const topic2 = await topicWith(['x']);
      const tp0 = new TopicPartition({ topic, partition: 0 });
      const t2p0 = new TopicPartition({ topic: topic2, partition: 0 });
      const cfg = configs({ 'enable.auto.commit': autoCommit, 'auto.commit.interval.ms': 500 });
      await commitFor(cfg, [[tp0, 0n]]);
      const consumer = new KafkaConsumer({ configs: cfg });
      try {
        await consumer.assign({ partitions: [tp0] });
        await consumer.seekToBeginning({ partitions: [tp0] });
        assert.equal(await committedOffset(consumer, tp0), 0n);
        assert.deepEqual([...consumer.assignment()].map(String), [String(tp0)]);
        const records = await pollRecords(consumer);
        assert.equal(records.count(), 1);
        assert.equal(await consumer.position({ partition: tp0 }), 1n);
        assert.equal(records.nextOffsets().get(tp0)?.offset, 1n);
        // Several auto-commit intervals, so the auto-commit (or the one
        // assign() makes once the interval has elapsed) has the position.
        await new Promise((resolve) => setTimeout(resolve, 1600));
        await consumer.assign({ partitions: [t2p0] });
        assert.deepEqual([...consumer.assignment()].map(String), [String(t2p0)]);
        // With auto-commit, the revoked partition's position is committed;
        // without it, no commit is sent, which a short wait checks.
        const expected = autoCommit ? 1n : 0n;
        const deadline = Date.now() + (autoCommit ? DEADLINE_MS : 2000);
        for (;;) {
          const offset = await committedOffset(consumer, tp0);
          if ((autoCommit && offset === expected) || Date.now() >= deadline) {
            assert.equal(offset, expected);
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      } finally {
        await closeNow(consumer);
      }
    },
    TIMEOUT,
  );
}

it(
  'testOffsetOfPausedPartitions',
  async () => {
    const topic = await topicWith(['a', 'b', 'c'], 2);
    const tp0 = new TopicPartition({ topic, partition: 0 });
    const tp1 = new TopicPartition({ topic, partition: 1 });
    const cfg = configs();
    await commitFor(cfg, [
      [tp0, 0n],
      [tp1, 0n],
    ]);
    const consumer = new KafkaConsumer({ configs: cfg });
    try {
      const partitions = new TopicPartitionSet([tp0, tp1]);
      await consumer.assign({ partitions });
      assert.deepEqual([...consumer.assignment()].map(String).sort(), [String(tp0), String(tp1)]);
      await consumer.pause({ partitions });
      await consumer.seekToEnd({ partitions });
      assert.equal(await committedOffset(consumer, tp0), 0n);
      assert.equal(await committedOffset(consumer, tp1), 0n);
      // The positions of paused partitions are resolved too.
      assert.equal(await consumer.position({ partition: tp0 }), 3n);
      assert.equal(await consumer.position({ partition: tp1 }), 3n);
      await consumer.unsubscribe();
    } finally {
      await closeNow(consumer);
    }
  },
  TIMEOUT,
);

it(
  'testListOffsetShouldUpdateSubscriptions',
  async () => {
    // Java seeks to 50 over 90 records and expects a lag of 40; here 1 over 3,
    // so the end offset endOffsets() stored gives a lag of 2.
    const topic = await topicWith(['a', 'b', 'c']);
    const tp = new TopicPartition({ topic, partition: 0 });
    const consumer = new KafkaConsumer({ configs: configs({ 'group.id': null }) });
    try {
      await consumer.assign({ partitions: [tp] });
      await consumer.seek({ partition: tp, offset: 1 });
      const ends = await consumer.endOffsets({ partitions: [tp] });
      assert.equal(ends.size(), 1);
      assert.equal(ends.get(tp), 3n);
      assert.equal(await consumer.currentLag({ topicPartition: tp }), 2n);
    } finally {
      await closeNow(consumer);
    }
  },
  TIMEOUT,
);

it(
  'verifyNoCoordinatorLookupForManualAssignmentWithSeek',
  async () => {
    // Without a group.id there is no coordinator to look up: the records come
    // from the offset lookup and the fetch alone.
    const topic = await topicWith(['a', 'b', 'c']);
    const tp = new TopicPartition({ topic, partition: 0 });
    const consumer = new KafkaConsumer({ configs: configs({ 'group.id': null }) });
    try {
      await consumer.assign({ partitions: [tp] });
      await consumer.seekToBeginning({ partitions: [tp] });
      const records = await pollRecords(consumer);
      assert.equal(records.count(), 3);
      assert.equal(await consumer.position({ partition: tp }), 3n);
      assert.equal(records.nextOffsets().get(tp)?.offset, 3n);
    } finally {
      await closeNow(consumer);
    }
  },
  TIMEOUT,
);

it(
  'testPollSendsRequestToJoin',
  async () => {
    // Java checks that subscribe() sends no heartbeat and poll() does; the
    // requests cannot be seen here, so the join poll() starts is: the member
    // gets its assignment only by polling.
    const topic = await topicWith(['a']);
    const tp = new TopicPartition({ topic, partition: 0 });
    const consumer = new KafkaConsumer({ configs: configs() });
    try {
      await consumer.subscribe({ topics: [topic] });
      await new Promise((resolve) => setTimeout(resolve, 1000));
      assert.equal(consumer.assignment().size(), 0);
      const deadline = Date.now() + DEADLINE_MS;
      while (!consumer.assignment().has(tp)) {
        assert.ok(Date.now() < deadline, 'the member did not join');
        await consumer.poll({ timeout: 0 });
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    } finally {
      await closeNow(consumer);
    }
  },
  TIMEOUT,
);
