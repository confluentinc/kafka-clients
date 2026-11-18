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

// A short stress of the producer against a broker, for CI (a few minutes at
// most): many records in batches whose sends are awaited together (root
// conventions, Threads and callbacks: several calls in flight at once, awaited
// together, work on the producer), under buffer pressure, and many producers
// made, used and closed one after another. Each checks that nothing crashes,
// every record's callback runs exactly once, every future completes, each
// record's headers still read back once it is sent (the native views of the
// sealed headers, while the collector runs), and the RSS stays bounded once
// warm. Every CANARY-th record goes to a one-partition topic of its own, read
// back from the broker at the end: each of them is there once, with its
// header, without reading back the whole run. STRESS_RECORDS sets the number
// of records of the first (default 1,000,000), STRESS_PRODUCERS the number of
// producers of the second (default 50).

const assert = require('node:assert/strict');

const { Header } = require('@confluentinc/kafka-javascript/common/header');
const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');
const { createTopic, fetchRecords, waitForBroker } = require('../integration/_fetch_records');

const BOOTSTRAP = /** @type {string} */ (process.env.KAFKA_BOOTSTRAP_SERVERS);
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to produce to';
if (SKIP) console.log(`skipping the stress tests: ${SKIP}`);

// The broker the Makefile has just started may not listen yet.
beforeAll(async () => {
  if (!SKIP) await waitForBroker(BOOTSTRAP);
}, 90_000);

const RECORDS = Number(process.env.STRESS_RECORDS ?? 1_000_000);
const PRODUCERS = Number(process.env.STRESS_PRODUCERS ?? 50);
const BATCH = 10_000;
const CANARY = 1000;
const MIB = 1024 * 1024;
// A call of the producer that has not settled after this long fails the test
// with what it was and how far its records got, rather than only with the
// test's own timeout.
const STALL_MS = 60_000;

let topicCount = 0;
/** @param {string} what */
function freshTopic(what) {
  return `js-stress-${what}-${process.pid}-${Date.now()}-${topicCount++}`;
}

/**
 * `promise`, or a failure naming the call when it has not settled after STALL_MS.
 *
 * @template T
 * @param {Promise<T>} promise the call's Promise
 * @param {() => string} what the call and its progress, read when it stalls
 * @returns {Promise<T>} what the call settles with
 */
async function within(promise, what) {
  /** @type {NodeJS.Timeout | undefined} */
  let timer;
  const stalled = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${what()}: not settled after ${STALL_MS / 1000} s`)), STALL_MS);
  });
  try {
    return await Promise.race([promise, stalled]);
  } finally {
    clearTimeout(timer);
  }
}

/**
 * The topics of a run: `main`, and `canary`, which every CANARY-th record goes to.
 *
 * @typedef {{ main: string, canary: string }} Topics
 */

/**
 * Creates the topics of a run.
 *
 * @param {string} what the run
 * @param {number} partitions the main topic's partitions
 * @returns {Promise<Topics>} the topics
 */
async function createTopics(what, partitions) {
  const topics = { main: freshTopic(what), canary: freshTopic(`${what}-canary`) };
  await createTopic(BOOTSTRAP, topics.main, { partitions });
  await createTopic(BOOTSTRAP, topics.canary);
  return topics;
}

/**
 * The record of index `i`: its key and a header carry the index.
 *
 * @param {Topics} topics the topics
 * @param {number} i the index
 * @returns {ProducerRecord<Buffer, Buffer>} the record
 */
function recordOf(topics, i) {
  const index = Buffer.from(String(i));
  return new ProducerRecord({
    topic: i % CANARY === 0 ? topics.canary : topics.main,
    partition: null,
    key: index,
    value: Buffer.alloc(100, i % 256),
    headers: [new Header({ key: 'i', value: index })],
  });
}

/**
 * Sends records `from` to `to - 1`, all at once, each with a callback that
 * counts its runs in `runs`, and waits for every send and every delivery;
 * then reads each record's header back, which the send has made read-only.
 *
 * @param {KafkaProducer<Buffer, Buffer>} producer the producer
 * @param {Topics} topics the topics
 * @param {number} from the first index
 * @param {number} to after the last index
 * @param {Uint8Array} runs the callback runs, by index
 * @param {unknown[]} failures where a delivery failure goes
 */
async function sendBatch(producer, topics, from, to, runs, failures) {
  const records = [];
  const sends = [];
  const progress = { accepted: 0, callbacks: 0, delivered: 0 };
  const where = (/** @type {string} */ call) =>
    `${call} for records ${from} to ${to - 1}: ${progress.accepted} accepted, ${progress.callbacks} callbacks, ${progress.delivered} futures done`;
  for (let i = from; i < to; i++) {
    const record = recordOf(topics, i);
    records.push(record);
    sends.push(
      producer
        .send({
          record,
          callback: ({ exception }) => {
            runs[i]++;
            progress.callbacks++;
            if (exception !== null) failures.push(exception);
          },
        })
        .then((future) => {
          progress.accepted++;
          return future;
        }),
    );
  }
  const futures = await within(Promise.all(sends), () => where('send()'));
  await within(Promise.all(futures.map((f) => f.get().then(() => void progress.delivered++))), () => where('get()'));
  records.forEach((record, n) => {
    const value = record.headers.lastHeader('i')?.value();
    if (value?.toString() !== String(from + n)) assert.fail(`record ${from + n} reads back the header ${value?.toString()}`);
  });
}

/**
 * Checks the canary topic holds each CANARY-th record of `count` once, with
 * its header; in any order, as sends in flight together have none.
 *
 * @param {Topics} topics the topics
 * @param {number} count the records of the run
 */
async function assertCanaries(topics, count) {
  const { records } = await fetchRecords(BOOTSTRAP, topics.canary, 0);
  const expected = [];
  for (let i = 0; i < count; i += CANARY) expected.push(i);
  assert.deepEqual(
    records.map((r) => Number(r.key?.toString())).sort((a, b) => a - b),
    expected,
  );
  for (const r of records) {
    assert.deepEqual(
      r.headers.map((h) => [h.key, h.value?.toString()]),
      [['i', r.key?.toString()]],
    );
  }
}

(SKIP ? test.skip : test)(
  `${RECORDS} records in batches of ${BATCH}, sent together under buffer pressure: every callback once, the headers read back, the RSS bounded`,
  async () => {
    const topics = await createTopics('batches', 4);
    // A buffer that holds a fraction of a batch: the sends wait for memory.
    const producer = new KafkaProducer({
      'bootstrap.servers': BOOTSTRAP,
      'buffer.memory': 1 * MIB,
      'batch.size': 64 * 1024,
      'linger.ms': 5,
      'max.block.ms': 60_000,
    });
    const runs = new Uint8Array(RECORDS);
    /** @type {unknown[]} */
    const failures = [];
    /** @type {number[]} */
    const rss = [];
    let sent = false;
    try {
      for (let from = 0; from < RECORDS; from += BATCH) {
        await sendBatch(producer, topics, from, Math.min(from + BATCH, RECORDS), runs, failures);
        rss.push(process.memoryUsage().rss);
      }
      await within(producer.flush(), () => 'flush()');
      sent = true;
    } finally {
      // A close() that stalls after a failure above leaves that failure the test's.
      await within(producer.close(), () => 'close()').catch((e) => {
        if (sent) throw e;
      });
    }
    assert.deepEqual(failures, []);
    const notOnce = runs.findIndex((n) => n !== 1);
    assert.equal(notOnce, -1, `the callback of record ${notOnce} ran ${runs[notOnce]} times`);
    // Warm after the first tenth: from there the RSS may not grow by more than 256 MiB.
    const warm = rss.slice(Math.floor(rss.length / 10));
    const growth = (Math.max(...warm) - warm[0]) / MIB;
    assert.ok(growth < 256, `the RSS grew ${growth.toFixed(0)} MiB after the warm-up`);
    await assertCanaries(topics, RECORDS);
  },
  180_000,
);

(SKIP ? test.skip : test)(
  `${PRODUCERS} producers made, used and closed one after another: every callback once, the headers read back, the RSS bounded`,
  async () => {
    const topics = await createTopics('churn', 1);
    const perProducer = 2_000;
    const runs = new Uint8Array(PRODUCERS * perProducer);
    /** @type {unknown[]} */
    const failures = [];
    /** @type {number[]} */
    const rss = [];
    for (let p = 0; p < PRODUCERS; p++) {
      const producer = new KafkaProducer({ 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 1 });
      let sent = false;
      try {
        await sendBatch(producer, topics, p * perProducer, (p + 1) * perProducer, runs, failures);
        sent = true;
      } finally {
        await within(producer.close(), () => `close() of producer ${p}`).catch((e) => {
          if (sent) throw e;
        });
      }
      rss.push(process.memoryUsage().rss);
    }
    assert.deepEqual(failures, []);
    const notOnce = runs.findIndex((n) => n !== 1);
    assert.equal(notOnce, -1, `the callback of record ${notOnce} ran ${runs[notOnce]} times`);
    const warm = rss.slice(Math.floor(rss.length / 5));
    const growth = (Math.max(...warm) - warm[0]) / MIB;
    assert.ok(growth < 128, `the RSS grew ${growth.toFixed(0)} MiB over ${warm.length} producers after the warm-up`);
    await assertCanaries(topics, PRODUCERS * perProducer);
  },
  180_000,
);
